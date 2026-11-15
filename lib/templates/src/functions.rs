//! Function calls in templates.
//!
//! The syntax and semantics follow Frog (Marlen Jarholt, "Frog: Functions for ontologies -- An
//! extension for the OTTR-framework", Master's thesis, University of Oslo, 2022,
//! <https://hdl.handle.net/10852/96925>): a function call is a term, written Lisp-style as
//! `(function arg1 arg2 ...)`, that may be used wherever a template argument may be used. Its
//! arguments are terms (variables, constants or function calls), separated by whitespace.
//!
//! Frog's base functions are XPath functions and operators. The SPARQL built-in functions are
//! defined in terms of the same XPath functions and operators, so maplib uses its vectorized
//! SPARQL expression evaluator as the base function library: a function call is translated to a
//! SPARQL expression, which is evaluated over the columns of the template arguments. Functions
//! defined with Frog's `def` syntax are inlined into the expression.

use crate::ast::{
    ConstantTerm, ConstantTermOrList, Function, FunctionCall, FunctionName, StottrTerm, Template,
    FROG_FUNCTION_NS,
};
use oxrdf::{NamedNode, Variable};
use spargebra::algebra::{Expression, Function as SparqlFunction};
use std::collections::HashMap;
use thiserror::Error;

/// The variable that the constant `none` is translated to. It is never bound, so the SPARQL
/// evaluator treats it as an unbound value.
pub const NONE_VARIABLE: &str = "__ottr_function_none";

/// Functions defined in the template dataset, by IRI.
pub type FunctionLibrary = HashMap<String, Function>;

#[derive(Error, Debug, Clone, PartialEq)]
pub enum FunctionError {
    #[error("Function {0} is called with {1} arguments, but {2}")]
    WrongArity(String, usize, String),
    #[error("Function {0} is recursive, which is not supported (call chain: {1})")]
    Recursive(String, String),
    #[error("Variable {1} used in the body of function {0} is not a parameter of the function")]
    UndefinedParameter(String, String),
    #[error("Blank node {0} can not be used as an argument of function {1}")]
    BlankNodeArgument(String, String),
    #[error("Lists can not be used as arguments of function {0}")]
    ListArgument(String),
    #[error("The argument of BOUND in {0} must be a variable")]
    BoundNotVariable(String),
    #[error("The Frog base function {0} has no SPARQL counterpart in maplib")]
    UnsupportedFrogFunction(String),
    #[error("Argument {0} of template {1} is a function call, which can not be list expanded")]
    ListExpandFunctionCall(usize, String),
}

/// The SPARQL built-in functions and operators that can be used as function names, in upper
/// case. `URI` is the SPARQL synonym of `IRI`.
const BUILTINS: &[&str] = &[
    "STR",
    "LANG",
    "LANGMATCHES",
    "DATATYPE",
    "IRI",
    "URI",
    "BNODE",
    "RAND",
    "ABS",
    "CEIL",
    "FLOOR",
    "ROUND",
    "CONCAT",
    "SUBSTR",
    "STRLEN",
    "REPLACE",
    "UCASE",
    "LCASE",
    "ENCODE_FOR_URI",
    "CONTAINS",
    "STRSTARTS",
    "STRENDS",
    "STRBEFORE",
    "STRAFTER",
    "YEAR",
    "MONTH",
    "DAY",
    "HOURS",
    "MINUTES",
    "SECONDS",
    "TIMEZONE",
    "TZ",
    "NOW",
    "UUID",
    "STRUUID",
    "MD5",
    "SHA1",
    "SHA256",
    "SHA384",
    "SHA512",
    "STRLANG",
    "STRDT",
    "ISIRI",
    "ISURI",
    "ISBLANK",
    "ISLITERAL",
    "ISNUMERIC",
    "REGEX",
    "IF",
    "COALESCE",
    "SAMETERM",
    "IN",
    "NOT_IN",
    "BOUND",
    "||",
    "&&",
    "=",
    "!=",
    "<",
    ">",
    "<=",
    ">=",
    "+",
    "-",
    "*",
    "/",
    "!",
];

pub fn is_builtin_name(name: &str) -> bool {
    BUILTINS.contains(&name.to_uppercase().as_str())
}

/// Translates a function call term to the SPARQL expression that computes it. Variables in the
/// expression are the template variables that the call refers to.
pub fn function_call_to_expression(
    function_call: &FunctionCall,
    functions: &FunctionLibrary,
) -> Result<Expression, FunctionError> {
    let mut stack = vec![];
    translate_call(function_call, None, functions, &mut stack)
}

/// The template with the calls of functions defined with `def` replaced by the function bodies,
/// and every function call checked. Used for the templates of virtualized resources, whose
/// function calls are translated without the function library when queries are prepared.
pub fn inline_template_functions(
    template: &Template,
    functions: &FunctionLibrary,
) -> Result<Template, FunctionError> {
    let mut template = template.clone();
    for i in &mut template.pattern_list {
        for a in &mut i.argument_list {
            if let StottrTerm::FunctionCall(fc) = &a.term {
                let mut stack = vec![];
                let inlined = inline_call(fc, None, functions, &mut stack)?;
                function_call_to_expression(&inlined, &FunctionLibrary::new())?;
                a.term = StottrTerm::FunctionCall(inlined);
            }
        }
    }
    Ok(template)
}

fn inline_term(
    term: &StottrTerm,
    env: Option<&HashMap<Variable, StottrTerm>>,
    functions: &FunctionLibrary,
    stack: &mut Vec<String>,
) -> Result<StottrTerm, FunctionError> {
    match term {
        StottrTerm::Variable(v) => match env {
            Some(env) => env.get(v).cloned().ok_or_else(|| {
                FunctionError::UndefinedParameter(
                    stack.last().cloned().unwrap_or_default(),
                    v.to_string(),
                )
            }),
            None => Ok(term.clone()),
        },
        StottrTerm::FunctionCall(fc) => Ok(StottrTerm::FunctionCall(inline_call(
            fc, env, functions, stack,
        )?)),
        _ => Ok(term.clone()),
    }
}

fn inline_call(
    function_call: &FunctionCall,
    env: Option<&HashMap<Variable, StottrTerm>>,
    functions: &FunctionLibrary,
    stack: &mut Vec<String>,
) -> Result<FunctionCall, FunctionError> {
    let mut arguments = vec![];
    for a in &function_call.arguments {
        arguments.push(inline_term(a, env, functions, stack)?);
    }
    if let FunctionName::Iri(nn) = &function_call.function {
        if let Some(f) = functions.get(nn.as_str()) {
            if stack.iter().any(|x| x == nn.as_str()) {
                let mut chain = stack.clone();
                chain.push(nn.as_str().to_string());
                return Err(FunctionError::Recursive(nn.to_string(), chain.join(" -> ")));
            }
            if f.parameter_list.len() != arguments.len() {
                return Err(FunctionError::WrongArity(
                    nn.to_string(),
                    arguments.len(),
                    format!("expected {}", f.parameter_list.len()),
                ));
            }
            let inner_env: HashMap<Variable, StottrTerm> = f
                .parameter_list
                .iter()
                .map(|p| p.variable.clone())
                .zip(arguments)
                .collect();
            stack.push(nn.as_str().to_string());
            let r = inline_call(&f.body, Some(&inner_env), functions, stack);
            stack.pop();
            return r;
        }
    }
    Ok(FunctionCall {
        function: function_call.function.clone(),
        type_arguments: function_call.type_arguments.clone(),
        arguments,
    })
}

fn translate_term(
    term: &StottrTerm,
    env: Option<&HashMap<Variable, Expression>>,
    functions: &FunctionLibrary,
    stack: &mut Vec<String>,
    calling: &str,
) -> Result<Expression, FunctionError> {
    match term {
        StottrTerm::Variable(v) => {
            if let Some(env) = env {
                if let Some(e) = env.get(v) {
                    Ok(e.clone())
                } else {
                    Err(FunctionError::UndefinedParameter(
                        stack.last().cloned().unwrap_or_default(),
                        v.to_string(),
                    ))
                }
            } else {
                Ok(Expression::Variable(v.clone()))
            }
        }
        StottrTerm::ConstantTerm(ConstantTermOrList::ConstantTerm(ct)) => match ct {
            ConstantTerm::Iri(nn) => Ok(Expression::NamedNode(nn.clone())),
            ConstantTerm::Literal(l) => Ok(Expression::Literal(l.clone())),
            ConstantTerm::None => Ok(Expression::Variable(Variable::new_unchecked(
                NONE_VARIABLE,
            ))),
            ConstantTerm::BlankNode(b) => Err(FunctionError::BlankNodeArgument(
                b.to_string(),
                calling.to_string(),
            )),
        },
        StottrTerm::ConstantTerm(ConstantTermOrList::ConstantList(_)) | StottrTerm::List(_) => {
            Err(FunctionError::ListArgument(calling.to_string()))
        }
        StottrTerm::FunctionCall(fc) => translate_call(fc, env, functions, stack),
    }
}

fn function_display_name(name: &FunctionName) -> String {
    match name {
        FunctionName::Iri(nn) => nn.to_string(),
        FunctionName::Builtin(b) => b.clone(),
    }
}

fn translate_call(
    function_call: &FunctionCall,
    env: Option<&HashMap<Variable, Expression>>,
    functions: &FunctionLibrary,
    stack: &mut Vec<String>,
) -> Result<Expression, FunctionError> {
    let name = function_display_name(&function_call.function);
    // BOUND takes a variable, not an expression.
    if let FunctionName::Builtin(b) = &function_call.function {
        if b == "BOUND" {
            if function_call.arguments.len() != 1 {
                return Err(FunctionError::WrongArity(
                    name,
                    function_call.arguments.len(),
                    "expected 1".to_string(),
                ));
            }
            return match translate_term(
                function_call.arguments.first().unwrap(),
                env,
                functions,
                stack,
                &name,
            )? {
                Expression::Variable(v) => Ok(Expression::Bound(v)),
                _ => Err(FunctionError::BoundNotVariable(name)),
            };
        }
    }
    let mut args = Vec::with_capacity(function_call.arguments.len());
    for a in &function_call.arguments {
        args.push(translate_term(a, env, functions, stack, &name)?);
    }
    match &function_call.function {
        FunctionName::Builtin(b) => builtin_expression(b, args),
        FunctionName::Iri(nn) => {
            if let Some(f) = functions.get(nn.as_str()) {
                if stack.iter().any(|x| x == nn.as_str()) {
                    let mut chain = stack.clone();
                    chain.push(nn.as_str().to_string());
                    return Err(FunctionError::Recursive(nn.to_string(), chain.join(" -> ")));
                }
                if f.parameter_list.len() != args.len() {
                    return Err(FunctionError::WrongArity(
                        nn.to_string(),
                        args.len(),
                        format!("expected {}", f.parameter_list.len()),
                    ));
                }
                let inner_env: HashMap<Variable, Expression> = f
                    .parameter_list
                    .iter()
                    .map(|p| p.variable.clone())
                    .zip(args)
                    .collect();
                stack.push(nn.as_str().to_string());
                let e = translate_call(&f.body, Some(&inner_env), functions, stack);
                stack.pop();
                e
            } else if let Some(local) = nn.as_str().strip_prefix(FROG_FUNCTION_NS) {
                frog_base_function_expression(nn, local, args)
            } else {
                // E.g. xsd casts such as xsd:integer(?x), or other functions the SPARQL engine
                // knows, such as GeoSPARQL functions and registered user defined functions.
                Ok(Expression::FunctionCall(
                    SparqlFunction::Custom(nn.clone()),
                    args,
                ))
            }
        }
    }
}

/// Frog base functions that have a SPARQL counterpart. The names are those of the base function
/// library of the Frog implementation in Lutra (https://gitlab.com/marlenjarholt/lutra).
fn frog_base_function_expression(
    nn: &NamedNode,
    local: &str,
    args: Vec<Expression>,
) -> Result<Expression, FunctionError> {
    let cast = match local {
        "decimal" => Some(oxrdf::vocab::xsd::DECIMAL),
        "number" => Some(oxrdf::vocab::xsd::DOUBLE),
        _ => None,
    };
    if let Some(cast) = cast {
        arity(nn.as_str(), &args, &[1])?;
        return Ok(Expression::FunctionCall(
            SparqlFunction::Custom(cast.into_owned()),
            args,
        ));
    }
    let builtin = match local {
        "upper-case" => "UCASE",
        "lower-case" => "LCASE",
        "string-length" => "STRLEN",
        "substring" | "substringLength" => "SUBSTR",
        "string" => "STR",
        "round" => "ROUND",
        "plus" => "+",
        "minus" => "-",
        "times" => "*",
        "divide" => "/",
        "concat" => "CONCAT",
        "castToIRI" => "IRI",
        "if" => "IF",
        "equal" => "=",
        "greaterThan" => ">",
        "lessThan" => "<",
        "or" => "||",
        "and" => "&&",
        "not" => "!",
        _ => return Err(FunctionError::UnsupportedFrogFunction(nn.to_string())),
    };
    builtin_expression(builtin, args).map_err(|e| match e {
        FunctionError::WrongArity(_, given, expected) => {
            FunctionError::WrongArity(nn.to_string(), given, expected)
        }
        e => e,
    })
}

fn arity(name: &str, args: &[Expression], allowed: &[usize]) -> Result<(), FunctionError> {
    if allowed.contains(&args.len()) {
        Ok(())
    } else {
        let expected = allowed
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(" or ");
        Err(FunctionError::WrongArity(
            name.to_string(),
            args.len(),
            format!("expected {expected}"),
        ))
    }
}

fn at_least(name: &str, args: &[Expression], n: usize) -> Result<(), FunctionError> {
    if args.len() >= n {
        Ok(())
    } else {
        Err(FunctionError::WrongArity(
            name.to_string(),
            args.len(),
            format!("expected at least {n}"),
        ))
    }
}

fn binary(
    name: &str,
    mut args: Vec<Expression>,
    f: fn(Box<Expression>, Box<Expression>) -> Expression,
) -> Result<Expression, FunctionError> {
    arity(name, &args, &[2])?;
    let right = args.pop().unwrap();
    let left = args.pop().unwrap();
    Ok(f(Box::new(left), Box::new(right)))
}

/// Left fold of an associative operator over two or more arguments, e.g. `(+ 1 2 3)`.
fn fold(
    name: &str,
    args: Vec<Expression>,
    f: fn(Box<Expression>, Box<Expression>) -> Expression,
) -> Result<Expression, FunctionError> {
    at_least(name, &args, 2)?;
    let mut it = args.into_iter();
    let mut acc = it.next().unwrap();
    for a in it {
        acc = f(Box::new(acc), Box::new(a));
    }
    Ok(acc)
}

fn call(
    name: &str,
    f: SparqlFunction,
    args: Vec<Expression>,
    allowed: &[usize],
) -> Result<Expression, FunctionError> {
    arity(name, &args, allowed)?;
    Ok(Expression::FunctionCall(f, args))
}

fn builtin_expression(name: &str, mut args: Vec<Expression>) -> Result<Expression, FunctionError> {
    use SparqlFunction as F;
    match name {
        "||" => fold(name, args, Expression::Or),
        "&&" => fold(name, args, Expression::And),
        "+" => fold(name, args, Expression::Add),
        "*" => fold(name, args, Expression::Multiply),
        "-" => {
            if args.len() == 1 {
                Ok(Expression::UnaryMinus(Box::new(args.pop().unwrap())))
            } else {
                binary(name, args, Expression::Subtract)
            }
        }
        "/" => binary(name, args, Expression::Divide),
        "=" => binary(name, args, Expression::Equal),
        "!=" => {
            let e = binary(name, args, Expression::Equal)?;
            Ok(Expression::Not(Box::new(e)))
        }
        "<" => binary(name, args, Expression::Less),
        ">" => binary(name, args, Expression::Greater),
        "<=" => binary(name, args, Expression::LessOrEqual),
        ">=" => binary(name, args, Expression::GreaterOrEqual),
        "SAMETERM" => binary(name, args, Expression::SameTerm),
        "!" => {
            arity(name, &args, &[1])?;
            Ok(Expression::Not(Box::new(args.pop().unwrap())))
        }
        "IF" => {
            arity(name, &args, &[3])?;
            let e3 = args.pop().unwrap();
            let e2 = args.pop().unwrap();
            let e1 = args.pop().unwrap();
            Ok(Expression::If(Box::new(e1), Box::new(e2), Box::new(e3)))
        }
        "COALESCE" => {
            at_least(name, &args, 1)?;
            Ok(Expression::Coalesce(args))
        }
        "IN" | "NOT_IN" => {
            at_least(name, &args, 1)?;
            let first = args.remove(0);
            let e = Expression::In(Box::new(first), args);
            if name == "IN" {
                Ok(e)
            } else {
                Ok(Expression::Not(Box::new(e)))
            }
        }
        "STR" => call(name, F::Str, args, &[1]),
        "LANG" => call(name, F::Lang, args, &[1]),
        "LANGMATCHES" => call(name, F::LangMatches, args, &[2]),
        "DATATYPE" => call(name, F::Datatype, args, &[1]),
        "IRI" | "URI" => call(name, F::Iri, args, &[1]),
        "BNODE" => call(name, F::BNode, args, &[0, 1]),
        "RAND" => call(name, F::Rand, args, &[0]),
        "ABS" => call(name, F::Abs, args, &[1]),
        "CEIL" => call(name, F::Ceil, args, &[1]),
        "FLOOR" => call(name, F::Floor, args, &[1]),
        "ROUND" => call(name, F::Round, args, &[1]),
        "CONCAT" => Ok(Expression::FunctionCall(F::Concat, args)),
        "SUBSTR" => call(name, F::SubStr, args, &[2, 3]),
        "STRLEN" => call(name, F::StrLen, args, &[1]),
        "REPLACE" => call(name, F::Replace, args, &[3, 4]),
        "UCASE" => call(name, F::UCase, args, &[1]),
        "LCASE" => call(name, F::LCase, args, &[1]),
        "ENCODE_FOR_URI" => call(name, F::EncodeForUri, args, &[1]),
        "CONTAINS" => call(name, F::Contains, args, &[2]),
        "STRSTARTS" => call(name, F::StrStarts, args, &[2]),
        "STRENDS" => call(name, F::StrEnds, args, &[2]),
        "STRBEFORE" => call(name, F::StrBefore, args, &[2]),
        "STRAFTER" => call(name, F::StrAfter, args, &[2]),
        "YEAR" => call(name, F::Year, args, &[1]),
        "MONTH" => call(name, F::Month, args, &[1]),
        "DAY" => call(name, F::Day, args, &[1]),
        "HOURS" => call(name, F::Hours, args, &[1]),
        "MINUTES" => call(name, F::Minutes, args, &[1]),
        "SECONDS" => call(name, F::Seconds, args, &[1]),
        "TIMEZONE" => call(name, F::Timezone, args, &[1]),
        "TZ" => call(name, F::Tz, args, &[1]),
        "NOW" => call(name, F::Now, args, &[0]),
        "UUID" => call(name, F::Uuid, args, &[0]),
        "STRUUID" => call(name, F::StrUuid, args, &[0]),
        "MD5" => call(name, F::Md5, args, &[1]),
        "SHA1" => call(name, F::Sha1, args, &[1]),
        "SHA256" => call(name, F::Sha256, args, &[1]),
        "SHA384" => call(name, F::Sha384, args, &[1]),
        "SHA512" => call(name, F::Sha512, args, &[1]),
        "STRLANG" => call(name, F::StrLang, args, &[2]),
        "STRDT" => call(name, F::StrDt, args, &[2]),
        "ISIRI" | "ISURI" => call(name, F::IsIri, args, &[1]),
        "ISBLANK" => call(name, F::IsBlank, args, &[1]),
        "ISLITERAL" => call(name, F::IsLiteral, args, &[1]),
        "ISNUMERIC" => call(name, F::IsNumeric, args, &[1]),
        "REGEX" => call(name, F::Regex, args, &[2, 3]),
        _ => unreachable!("Builtin function names are checked by the parser"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::document_from_str;
    use crate::ast::Statement;
    use representation::prefixes::get_default_prefixes;

    fn parse_functions_and_call(doc: &str) -> (FunctionLibrary, FunctionCall) {
        let d = document_from_str(doc, Some(&get_default_prefixes())).unwrap();
        let mut functions = FunctionLibrary::new();
        let mut call = None;
        for s in d.statements {
            match s {
                Statement::Function(f) => {
                    functions.insert(f.iri.as_str().to_string(), f);
                }
                Statement::Template(t) => {
                    for i in t.pattern_list {
                        for a in i.argument_list {
                            if let StottrTerm::FunctionCall(fc) = a.term {
                                call = Some(fc);
                            }
                        }
                    }
                }
                Statement::Instance(_) => {}
            }
        }
        (functions, call.unwrap())
    }

    #[test]
    fn test_builtin_translation() {
        let (functions, call) = parse_functions_and_call(
            r#"
@prefix ex:<http://example.net/ns#>.
ex:T [?a] :: { ottr:Triple(ex:s, ex:p, (IRI (CONCAT "http://example.net/" (UCASE ?a)))) } .
"#,
        );
        let e = function_call_to_expression(&call, &functions).unwrap();
        assert_eq!(
            e.to_string(),
            r#"IRI(CONCAT("http://example.net/", UCASE(?a)))"#
        );
    }

    #[test]
    fn test_operators_fold_and_none() {
        let (functions, call) = parse_functions_and_call(
            r#"
@prefix ex:<http://example.net/ns#>.
ex:T [?a] :: { ottr:Triple(ex:s, ex:p, (COALESCE (+ ?a 1 2) "x")) } .
"#,
        );
        let e = function_call_to_expression(&call, &functions).unwrap();
        assert_eq!(
            e.to_string(),
            "COALESCE(((?a + \"1\"^^<http://www.w3.org/2001/XMLSchema#integer>) + \"2\"^^<http://www.w3.org/2001/XMLSchema#integer>), \"x\")"
        );
    }

    #[test]
    fn test_frog_function_inlining() {
        let (functions, call) = parse_functions_and_call(
            r#"
@prefix ex:<http://example.net/ns#>.
@prefix fn:<http://ns.frog.ottr.xyz/0.1/function/>.
def ex:toIRI(xsd:string ?namespace, xsd:string ?localName) -> ottr:IRI :: (
    fn:castToIRI (fn:concat ?namespace (REPLACE ?localName " " "_"))
) .
def ex:toIRIWNamespace(xsd:string ?localName) -> ottr:IRI :: (
    ex:toIRI "http://example.org/data/" ?localName
) .
ex:T [?name] :: { ottr:Triple((ex:toIRIWNamespace ?name), ex:p, ?name) } .
"#,
        );
        let e = function_call_to_expression(&call, &functions).unwrap();
        assert_eq!(
            e.to_string(),
            r#"IRI(CONCAT("http://example.org/data/", REPLACE(?name, " ", "_")))"#
        );
    }

    #[test]
    fn test_inline_template_functions() {
        let doc = r#"
@prefix ex:<http://example.net/ns#>.
def ex:kilo(xsd:double ?x) -> xsd:double :: (* ?x 1000) .
ex:T [?id, ?value] :: { ottr:Triple(?id, ex:p, (ex:kilo (xsd:double ?value))) } .
"#;
        let d = document_from_str(doc, Some(&get_default_prefixes())).unwrap();
        let mut functions = FunctionLibrary::new();
        let mut template = None;
        for s in d.statements {
            match s {
                Statement::Function(f) => {
                    functions.insert(f.iri.as_str().to_string(), f);
                }
                Statement::Template(t) => template = Some(t),
                Statement::Instance(_) => {}
            }
        }
        let t = inline_template_functions(&template.unwrap(), &functions).unwrap();
        let StottrTerm::FunctionCall(fc) = &t.pattern_list[0].argument_list[2].term else {
            panic!()
        };
        assert_eq!(
            function_call_to_expression(fc, &FunctionLibrary::new())
                .unwrap()
                .to_string(),
            "(<http://www.w3.org/2001/XMLSchema#double>(?value) * \"1000\"^^<http://www.w3.org/2001/XMLSchema#integer>)"
        );
    }

    #[test]
    fn test_recursive_function_is_rejected() {
        let (functions, call) = parse_functions_and_call(
            r#"
@prefix ex:<http://example.net/ns#>.
def ex:f(?x) -> xsd:integer :: (ex:g ?x) .
def ex:g(?x) -> xsd:integer :: (+ (ex:f ?x) 1) .
ex:T [?a] :: { ottr:Triple(ex:s, ex:p, (ex:f ?a)) } .
"#,
        );
        let e = function_call_to_expression(&call, &functions).unwrap_err();
        assert!(matches!(e, FunctionError::Recursive(..)), "{e}");
    }

    #[test]
    fn test_undefined_parameter_and_arity() {
        let (functions, call) = parse_functions_and_call(
            r#"
@prefix ex:<http://example.net/ns#>.
def ex:f(?x) -> xsd:integer :: (+ ?x ?y) .
ex:T [?a] :: { ottr:Triple(ex:s, ex:p, (ex:f ?a)) } .
"#,
        );
        let e = function_call_to_expression(&call, &functions).unwrap_err();
        assert!(matches!(e, FunctionError::UndefinedParameter(..)), "{e}");

        let (functions, call) = parse_functions_and_call(
            r#"
@prefix ex:<http://example.net/ns#>.
ex:T [?a] :: { ottr:Triple(ex:s, ex:p, (STRLEN ?a ?a)) } .
"#,
        );
        let e = function_call_to_expression(&call, &functions).unwrap_err();
        assert!(matches!(e, FunctionError::WrongArity(..)), "{e}");
    }

    #[test]
    fn test_unsupported_frog_function() {
        let (functions, call) = parse_functions_and_call(
            r#"
@prefix ex:<http://example.net/ns#>.
@prefix fn:<http://ns.frog.ottr.xyz/0.1/function/>.
ex:T [?a] :: { ottr:Triple(ex:s, ex:p, (fn:head ?a)) } .
"#,
        );
        let e = function_call_to_expression(&call, &functions).unwrap_err();
        assert!(matches!(e, FunctionError::UnsupportedFrogFunction(..)), "{e}");
    }
}
