#![allow(clippy::ignored_unit_patterns)]

use oxilangtag::LanguageTag;
use oxiri::{Iri, IriParseError};
use oxrdf::vocab::{xsd};
use peg::parser;
use peg::str::LineCol;
use std::char;
use std::collections::{HashMap, HashSet};
use oxrdf::{BlankNode, NamedNode, Variable, Literal};
use crate::ast::{PType, Annotation, Argument, StottrDocument, Template, ListExpanderType, Instance, Statement, Signature, Parameter, ConstantTerm, ConstantTermOrList, StottrTerm};
use crate::constants::{OTTR_PREFIX, OTTR_PREFIX_IRI, OWL_PREFIX, OWL_PREFIX_IRI, RDFS_PREFIX, RDFS_PREFIX_IRI, RDF_PREFIX, RDF_PREFIX_IRI, SHACL_PREFIX, SHACL_PREFIX_IRI, XSD_PREFIX, XSD_PREFIX_IRI};
use crate::dataset::errors::TemplateError;

pub fn parse_stottr(document: &str) -> Result<StottrDocument, TemplateError> {
    let mut state = ParserState::new();
    let doc = parser::StottrDocumentInit(document, &mut state)
        .map_err(|e| TemplateError::ParsingError(ParseErrorKind::Syntax(e)))?;
    println!("Doc {:?}", doc);
    Ok(doc)
}

#[derive(Debug, thiserror::Error)]
pub enum ParseErrorKind {
    #[error(transparent)]
    Syntax(#[from] peg::error::ParseError<LineCol>),
}

pub struct ParserState {
    namespaces: HashMap<String, String>,
    used_bnodes: HashSet<BlankNode>,
    currently_used_bnodes: HashSet<BlankNode>,
}

impl ParserState {
    pub(crate) fn new() -> Self {
        let predefined = [
            (RDFS_PREFIX, RDFS_PREFIX_IRI),
            (RDF_PREFIX, RDF_PREFIX_IRI),
            (XSD_PREFIX, XSD_PREFIX_IRI),
            (OTTR_PREFIX, OTTR_PREFIX_IRI),
            (OWL_PREFIX, OWL_PREFIX_IRI),
            (SHACL_PREFIX, SHACL_PREFIX_IRI),
        ];
        Self {
            namespaces: HashMap::from_iter(predefined.into_iter().map(|(x,y)|(x.to_string(), y.to_string()))),
            used_bnodes: HashSet::default(),
            currently_used_bnodes: HashSet::default(),
        }
    }

    fn parse_iri(&self, iri: String) -> Result<Iri<String>, IriParseError> {
        Iri::parse(iri)
    }
}

fn unescape_iriref(mut input: &str) -> Result<String, &'static str> {
    let mut output = String::with_capacity(input.len());
    while let Some((before, after)) = input.split_once('\\') {
        output.push_str(before);
        let mut after = after.chars();
        let (escape, after) = match after.next() {
            Some('u') => read_hex_char::<4>(after.as_str())?,
            Some('U') => read_hex_char::<8>(after.as_str())?,
            Some(_) => {
                return Err(
                    "IRIs are only allowed to contain escape sequences \\uXXXX and \\UXXXXXXXX",
                )
            }
            None => return Err("IRIs are not allowed to end with a '\'"),
        };
        output.push(escape);
        input = after;
    }
    output.push_str(input);
    Ok(output)
}

fn unescape_string(mut input: &str) -> Result<String, &'static str> {
    let mut output = String::with_capacity(input.len());
    while let Some((before, after)) = input.split_once('\\') {
        output.push_str(before);
        let mut after = after.chars();
        let (escape, after) = match after.next() {
            Some('t') => ('\u{0009}', after.as_str()),
            Some('b') => ('\u{0008}', after.as_str()),
            Some('n') => ('\u{000A}', after.as_str()),
            Some('r') => ('\u{000D}', after.as_str()),
            Some('f') => ('\u{000C}', after.as_str()),
            Some('"') => ('\u{0022}', after.as_str()),
            Some('\'') => ('\u{0027}', after.as_str()),
            Some('\\') => ('\u{005C}', after.as_str()),
            Some('u') => read_hex_char::<4>(after.as_str())?,
            Some('U') => read_hex_char::<8>(after.as_str())?,
            Some(_) => return Err("The character that can be escaped in strings are tbnrf\"'\\"),
            None => return Err("strings are not allowed to end with a '\'"),
        };
        output.push(escape);
        input = after;
    }
    output.push_str(input);
    Ok(output)
}

fn read_hex_char<const SIZE: usize>(input: &str) -> Result<(char, &str), &'static str> {
    if let Some(escape) = input.get(..SIZE) {
        if let Some(char) = u32::from_str_radix(escape, 16)
            .ok()
            .and_then(char::from_u32)
        {
            Ok((char, &input[SIZE..]))
        } else {
            Err("\\u escape sequence should be followed by hexadecimal digits")
        }
    } else {
        Err("\\u escape sequence should be followed by hexadecimal digits")
    }
}

parser! {
    //See https://www.w3.org/TR/turtle/#sec-grammar
    grammar parser(state: &mut ParserState) for str {
        pub rule StottrDocumentInit() -> StottrDocument = _ PrefixDeclList() _ statements:StottrStatements() _ {
            StottrDocument {
                directives: vec![],statements,prefix_map: state.namespaces.drain().map(|(x,y)|(x,NamedNode::new_unchecked(y))).collect()}
        }

        rule PrefixDeclList() = PrefixDecl() ** (_) {}

        rule PrefixDecl() = i("@PREFIX") _ ns:PNAME_NS() _ i:IRIREF() _ "." {
            state.namespaces.insert(ns.into(), i.into_inner());
            println!("ns {}", ns);
        }

        rule StottrStatements() -> Vec<Statement> = ss:StottrStatement() ** (_) {
            ss
        }

        rule StottrStatement() -> Statement =
        //    i:StottrInstance() {
        //    Statement::Instance(i)
        //} /
        t:StottrTemplate() {
            Statement::Template(t)
        }

        rule StottrInstance() -> Instance = list_expander:StottrExpander()? _  template_iri:iri() _ "(" _ argument_list:StottrArgumentList() _ ")" {
            Instance {
                list_expander, template_iri, argument_list,
            }
        }

        rule StottrArgumentList() -> Vec<Argument> = args:StottrArgument() ** ("," _) {
            args
        }

        rule StottrArgument() -> Argument = exp:"++"? _ term:StottrTerm() {
            Argument {list_expand: exp.is_some(),term }
        }

        rule StottrTerm() -> StottrTerm = v:Var() {
            StottrTerm::Variable(v)
        }  / cl:StottrConstantTermOrList() {
            StottrTerm::ConstantTerm(cl)
        } / c:StottrTermList() {
            StottrTerm::List(c)
        }

        rule StottrTermList() -> Vec<StottrTerm> = "(" _ ts:StottrTerm() ** ("," _) _ ")" {
            ts
        }

        rule StottrTemplate() -> Template = signature:StottrSignature() _ "::" _ "{" _ pattern_list:StottrPatternList() _ "}" _ "." {
            Template {signature,pattern_list}
        }

        rule StottrPatternList() -> Vec<Instance> = ts:StottrInstance() ** ("," _) {
            ts
        }

        rule StottrSignature() -> Signature =
            iri:iri() _ "[" _ parameter_list:StottrParameterList() _ "]" _ annotation_list:StottrAnnotationList()? {
            Signature {
                iri,
                parameter_list,
                annotation_list}
        }

        rule StottrAnnotationList() -> Vec<Annotation> = ans:StottrAnnotation() ** ("," _) {
            ans
        }

        rule StottrAnnotation() -> Annotation = "@@" _ instance:StottrInstance() {
            Annotation {instance}
        }

        rule StottrExpander() -> ListExpanderType = i("CROSS") _ "|" {
            ListExpanderType::Cross
        } / i("ZIPMIN") _ "|" {
            ListExpanderType::ZipMin
        } / i("ZIPMAX") _  "|" {
            ListExpanderType::ZipMax
        }

        rule StottrParameterList() -> Vec<Parameter> = ts:StottrParameter() ** ("," _){
            ts
        }

        rule StottrParameter() -> Parameter = p:StottrParameterNonOptional() {
            p
        } / p:StottrParameterOptional() {
            p
        }

        rule StottrParameterNonOptional() -> Parameter = pt:StottrPType()? _ nbl:"!"? _ v:Var() _ def:StottrConstantTermOrList()?  {
            Parameter {
                optional: false, non_blank:nbl.is_some(), ptype: pt, variable: v,default_value: def}
        }

        rule StottrParameterOptional() -> Parameter = "?" _ pt:StottrPType()? _ nbl:"!"? _ v:Var() _ def:StottrDefaultValue()?  {
            Parameter {
                optional: true, non_blank:nbl.is_some(), ptype: pt ,variable: v,default_value: def}
        }

        rule StottrDefaultValue() -> ConstantTermOrList = "=" _ def:StottrConstantTermOrList() {
            def
        }

        rule StottrPType() -> PType = ip:iri() {
            PType::Basic(ip)
        } / lt:StottrListPType() {
            lt
        } / lt:StottrNEListPType() {
            lt
        }

        rule StottrListPType() -> PType = i("LIST") _ "<" _ pt:StottrPType() _ ">" {
            PType::List(Box::new(pt))
        }

        rule StottrNEListPType() -> PType = i("NELIST") _ "<" _ pt:StottrPType() _ ">" {
            PType::NEList(Box::new(pt))
        }

        rule StottrLUBPType() -> PType = i("LUB") _ "<" _ pt:StottrPType() _ ">" {
            PType::Lub(Box::new(pt))
        }

        rule Var() -> Variable = name:(VAR1() / VAR2()) { Variable::new_unchecked(name) }

        rule StottrConstantTermOrList() -> ConstantTermOrList = ts:StottrConstantTermList() {
            ConstantTermOrList::ConstantList(ts.into_iter().map(|x|ConstantTermOrList::ConstantTerm(x)).collect())
        } / t:StottrConstantTerm() {
            ConstantTermOrList::ConstantTerm(t)
        }

        rule StottrConstantTermList() -> Vec<ConstantTerm> = "(" _ ts:StottrConstantTerm() ++ ("," _) _ ")" {
            ts
        }

        rule StottrConstantTerm() -> ConstantTerm =
            i:iri() { ConstantTerm::Iri(i) } /
            l:RDFLiteral() { ConstantTerm::Literal(l) } /
            l:NumericLiteral() { ConstantTerm::Literal(l) } /
            l:BooleanLiteral() { ConstantTerm::Literal(l) } /
            b:BlankNode() { ConstantTerm::BlankNode(b) }

        rule RDFLiteral() -> Literal =
            value:String() _ "^^" _ datatype:iri() { Literal::new_typed_literal(value, datatype) } /
            value:String() _ language:LANGTAG() { Literal::new_language_tagged_literal_unchecked(value, language.into_inner()) } /
            value:String() { Literal::new_simple_literal(value) }

        rule NumericLiteral() -> Literal  = NumericLiteralUnsigned() / NumericLiteralPositive() / NumericLiteralNegative()

        rule NumericLiteralUnsigned() -> Literal =
            d:$(DOUBLE()) { Literal::new_typed_literal(d, xsd::DOUBLE) } /
            d:$(DECIMAL()) { Literal::new_typed_literal(d, xsd::DECIMAL) } /
            i:$(INTEGER()) { Literal::new_typed_literal(i, xsd::INTEGER) }

        rule NumericLiteralPositive() -> Literal =
            d:$(DOUBLE_POSITIVE()) { Literal::new_typed_literal(d, xsd::DOUBLE) } /
            d:$(DECIMAL_POSITIVE()) { Literal::new_typed_literal(d, xsd::DECIMAL) } /
            i:$(INTEGER_POSITIVE()) { Literal::new_typed_literal(i, xsd::INTEGER) }


        rule NumericLiteralNegative() -> Literal =
            d:$(DOUBLE_NEGATIVE()) { Literal::new_typed_literal(d, xsd::DOUBLE) } /
            d:$(DECIMAL_NEGATIVE()) { Literal::new_typed_literal(d, xsd::DECIMAL) } /
            i:$(INTEGER_NEGATIVE()) { Literal::new_typed_literal(i, xsd::INTEGER) }

        rule BooleanLiteral() -> Literal =
            "true" { Literal::new_typed_literal("true", xsd::BOOLEAN) } /
            "false" { Literal::new_typed_literal("false", xsd::BOOLEAN) }

        rule String() -> String = STRING_LITERAL_LONG1() / STRING_LITERAL_LONG2() / STRING_LITERAL1() / STRING_LITERAL2()

        rule iri() -> NamedNode = i:(IRIREF() / PrefixedName()) {
            NamedNode::from(i)
        }

        rule PrefixedName() -> Iri<String> = PNAME_LN() /
            ns:PNAME_NS() {? if let Some(iri) = state.namespaces.get(ns).cloned() {
                Iri::parse(iri).map_err(|_| "IRI parsing failed")
            } else {
                println!("Prefix {}", ns);
                Err("Prefix not found")
            } }

        rule BlankNode() -> BlankNode = id:BLANK_NODE_LABEL() {?
            let node = BlankNode::new_unchecked(id);
            if state.used_bnodes.contains(&node) {
                Err("Already used blank node id")
            } else {
                state.currently_used_bnodes.insert(node.clone());
                Ok(node)
            }
        } / ANON() { BlankNode::default() }

        rule IRIREF() -> Iri<String> = "<" i:$((!['>'] [_])*) ">" {?
            state.parse_iri(unescape_iriref(i)?).map_err(|_| "IRI parsing failed")
        }

        rule PNAME_NS() -> &'input str = ns:$(PN_PREFIX()?) ":" {
            ns
        }

        rule PNAME_LN() -> Iri<String> = ns:PNAME_NS() local:$(PN_LOCAL()) {?
            if let Some(base) = state.namespaces.get(ns) {
                let mut iri = String::with_capacity(base.len() + local.len());
                iri.push_str(base);
                for chunk in local.split('\\') { // We remove \
                    iri.push_str(chunk);
                }
                Iri::parse(iri).map_err(|_| "IRI parsing failed")
            } else {
                Err("Prefix not found")
            }
        }

        rule BLANK_NODE_LABEL() -> &'input str = "_:" b:$((['0'..='9'] / PN_CHARS_U()) PN_CHARS()* ("."+ PN_CHARS()+)*) {
            b
        }

        rule VAR1() -> &'input str = "?" v:$(VARNAME()) { v }

        rule VAR2() -> &'input str = "$" v:$(VARNAME()) { v }

        rule LANGTAG() -> LanguageTag<String> = "@" l:$(['a' ..= 'z' | 'A' ..= 'Z']+ ("-" ['a' ..= 'z' | 'A' ..= 'Z' | '0' ..= '9']+)*) {?
            LanguageTag::parse(l.to_ascii_lowercase()).map_err(|_| "language tag parsing failed")
        }

        rule INTEGER() = ['0'..='9']+

        rule DECIMAL() = ['0'..='9']* "." ['0'..='9']+

        rule DOUBLE() = (['0'..='9']+ "." ['0'..='9']* / "." ['0'..='9']+ / ['0'..='9']+) EXPONENT()

        rule INTEGER_POSITIVE() = "+" _ INTEGER()

        rule DECIMAL_POSITIVE() = "+" _ DECIMAL()

        rule DOUBLE_POSITIVE() = "+" _ DOUBLE()

        rule INTEGER_NEGATIVE() = "-" _ INTEGER()

        rule DECIMAL_NEGATIVE() = "-" _ DECIMAL()

        rule DOUBLE_NEGATIVE() = "-" _ DOUBLE()

        rule EXPONENT() = ['e' | 'E'] ['+' | '-']? ['0'..='9']+

        rule STRING_LITERAL1() -> String = "'" l:$((STRING_LITERAL1_simple_char() / ECHAR() / UCHAR())*) "'" {?
             unescape_string(l)
        }
        rule STRING_LITERAL1_simple_char() = !['\u{27}' | '\u{5C}' | '\u{0A}' | '\u{0D}'] [_]


        rule STRING_LITERAL2() -> String = "\"" l:$((STRING_LITERAL2_simple_char() / ECHAR() / UCHAR())*) "\"" {?
             unescape_string(l)
        }
        rule STRING_LITERAL2_simple_char() = !['\u{22}' | '\u{5C}' | '\u{0A}' | '\u{0D}'] [_]

        rule STRING_LITERAL_LONG1() -> String = "'''" l:$(STRING_LITERAL_LONG1_inner()*) "'''" {?
             unescape_string(l)
        }
        rule STRING_LITERAL_LONG1_inner() = ("''" / "'")? (STRING_LITERAL_LONG1_simple_char() / ECHAR() / UCHAR())
        rule STRING_LITERAL_LONG1_simple_char() = !['\'' | '\\'] [_]

        rule STRING_LITERAL_LONG2() -> String = "\"\"\"" l:$(STRING_LITERAL_LONG2_inner()*) "\"\"\"" {?
             unescape_string(l)
        }
        rule STRING_LITERAL_LONG2_inner() = ("\"\"" / "\"")? (STRING_LITERAL_LONG2_simple_char() / ECHAR() / UCHAR())
        rule STRING_LITERAL_LONG2_simple_char() = !['"' | '\\'] [_]

        rule UCHAR() = "\\u" HEX() HEX() HEX() HEX() / "\\U" HEX() HEX() HEX() HEX() HEX() HEX() HEX() HEX()

        rule ECHAR() = "\\" ['t' | 'b' | 'n' | 'r' | 'f' | '"' |'\'' | '\\']

        rule NIL() = "(" WS()* ")"

        rule WS() = quiet! { ['\u{20}' | '\u{09}' | '\u{0D}' | '\u{0A}'] }

        rule ANON() = "[" WS()* "]"

        rule PN_CHARS_BASE() = ['A' ..= 'Z' | 'a' ..= 'z' | '\u{00C0}'..='\u{00D6}' | '\u{00D8}'..='\u{00F6}' | '\u{00F8}'..='\u{02FF}' | '\u{0370}'..='\u{037D}' | '\u{037F}'..='\u{1FFF}' | '\u{200C}'..='\u{200D}' | '\u{2070}'..='\u{218F}' | '\u{2C00}'..='\u{2FEF}' | '\u{3001}'..='\u{D7FF}' | '\u{F900}'..='\u{FDCF}' | '\u{FDF0}'..='\u{FFFD}']

        rule PN_CHARS_U() = ['_'] / PN_CHARS_BASE()

        rule VARNAME() = (['0'..='9'] / PN_CHARS_U()) (['0' ..= '9' | '\u{00B7}' | '\u{0300}'..='\u{036F}' | '\u{203F}'..='\u{2040}'] / PN_CHARS_U())*

        rule PN_CHARS() = ['-' | '0' ..= '9' | '\u{00B7}' | '\u{0300}'..='\u{036F}' | '\u{203F}'..='\u{2040}'] / PN_CHARS_U()

        rule PN_PREFIX() = PN_CHARS_BASE() PN_CHARS()* ("."+ PN_CHARS()+)*

        rule PN_LOCAL() = (PN_CHARS_U() / [':' | '0'..='9'] / PLX()) (PN_CHARS() / [':'] / PLX())* (['.']+ (PN_CHARS() / [':'] / PLX())+)?

        rule PLX() = PERCENT() / PN_LOCAL_ESC()

        rule PERCENT() = ['%'] HEX() HEX()

        rule HEX() = ['0' ..= '9' | 'A' ..= 'F' | 'a' ..= 'f']

        rule PN_LOCAL_ESC() = ['\\'] ['_' | '~' | '.' | '-' | '!' | '$' | '&' | '\'' | '(' | ')' | '*' | '+' | ',' | ';' | '=' | '/' | '?' | '#' | '@' | '%'] //TODO: added '/' to make tests pass but is it valid?

        //space
        rule _() = quiet! { ([' ' | '\t' | '\n' | '\r'] / comment())* }

        //comment
        rule comment() = quiet! { ['#'] (!['\r' | '\n'] [_])* }

        rule i(literal: &'static str) = input: $([_]*<{literal.len()}>) {?
            if input.eq_ignore_ascii_case(literal) {
                Ok(())
            } else {
                Err(literal)
            }
        }
    }
}
