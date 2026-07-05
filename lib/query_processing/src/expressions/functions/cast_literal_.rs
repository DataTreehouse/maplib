use oxrdf::vocab::xsd;
use oxrdf::NamedNodeRef;
use polars::datatypes::DataType;
use polars::prelude::{lit, Expr, StrptimeOptions};
use representation::cats::{maybe_decode_expr, LockedCats};
use representation::solution_mapping::BaseCatState;
use representation::BaseRDFNodeType;

pub fn cast_literal(
    mut c: Expr,
    src_bt: &BaseRDFNodeType,
    src_bs: &BaseCatState,
    global_cats: LockedCats,
    src: NamedNodeRef,
    trg: NamedNodeRef,
    trg_type: DataType,
) -> Expr {
    // Works around issue where src is string and trg is string.
    // TODO: Handle state comprehensively
    if src == xsd::STRING {
        c = maybe_decode_expr(c, src_bt, src_bs, global_cats);
    }
    if src == xsd::STRING && trg == xsd::BOOLEAN {
        c.cast(DataType::String)
            .str()
            .to_lowercase()
            .eq(lit("true"))
    } else if src == xsd::STRING && trg == xsd::DATE_TIME {
        c.cast(DataType::String).str().to_datetime(
            None,
            None,
            StrptimeOptions {
                format: None,
                strict: true,
                exact: false,
                cache: false,
            },
            lit("raise"),
        )
    } else if src == xsd::STRING && trg == xsd::DATE {
        c.cast(DataType::String).str().to_date(StrptimeOptions {
            format: None,
            strict: true,
            exact: false,
            cache: false,
        })
    } else if src == xsd::STRING && trg == xsd::TIME {
        c.cast(DataType::String).str().to_time(StrptimeOptions {
            format: None,
            strict: true,
            exact: false,
            cache: false,
        })
    } else if src == xsd::STRING && trg == xsd::DURATION {
        //Todo handle durations
        c
    } else {
        c.cast(trg_type)
    }
}
