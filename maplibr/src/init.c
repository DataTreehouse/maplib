
// clang-format sorts includes unless SortIncludes: Never. However, the ordering
// does matter here. So, we need to disable clang-format for safety.

// clang-format off
#include <stdint.h>
#include <Rinternals.h>
#include <R_ext/Parse.h>
// clang-format on

#include "rust/api.h"

static uintptr_t TAGGED_POINTER_MASK = (uintptr_t)1;

SEXP handle_result(SEXP res_) {
    uintptr_t res = (uintptr_t)res_;

    // An error is indicated by tag.
    if ((res & TAGGED_POINTER_MASK) == 1) {
        // Remove tag
        SEXP res_aligned = (SEXP)(res & ~TAGGED_POINTER_MASK);

        // Currently, there are two types of error cases:
        //
        //   1. Error from Rust code
        //   2. Error from R's C API, which is caught by R_UnwindProtect()
        //
        if (TYPEOF(res_aligned) == CHARSXP) {
            // In case 1, the result is an error message that can be passed to
            // Rf_errorcall() directly.
            Rf_errorcall(R_NilValue, "%s", CHAR(res_aligned));
        } else {
            // In case 2, the result is the token to restart the
            // cleanup process on R's side.
            R_ContinueUnwind(res_aligned);
        }
    }

    return (SEXP)res;
}

SEXP savvy_export_test_series__impl(SEXP c_arg__stream_ptr) {
    SEXP res = savvy_export_test_series__ffi(c_arg__stream_ptr);
    return handle_result(res);
}

SEXP savvy_export_test_solution_mappings__impl(SEXP c_arg__stream_ptr) {
    SEXP res = savvy_export_test_solution_mappings__ffi(c_arg__stream_ptr);
    return handle_result(res);
}

SEXP savvy_import_test_series__impl(SEXP c_arg__stream_ptr) {
    SEXP res = savvy_import_test_series__ffi(c_arg__stream_ptr);
    return handle_result(res);
}

SEXP savvy_import_test_solution_mappings__impl(SEXP c_arg__stream_ptr, SEXP c_arg__rdf_node_types_json) {
    SEXP res = savvy_import_test_solution_mappings__ffi(c_arg__stream_ptr, c_arg__rdf_node_types_json);
    return handle_result(res);
}

SEXP savvy_make_triple__impl(SEXP c_arg__subject, SEXP c_arg__predicate, SEXP c_arg__object, SEXP c_arg__list_expander) {
    SEXP res = savvy_make_triple__ffi(c_arg__subject, c_arg__predicate, c_arg__object, c_arg__list_expander);
    return handle_result(res);
}

SEXP savvy_validate_blank_node_id__impl(SEXP c_arg__id) {
    SEXP res = savvy_validate_blank_node_id__ffi(c_arg__id);
    return handle_result(res);
}

SEXP savvy_validate_iri__impl(SEXP c_arg__iri) {
    SEXP res = savvy_validate_iri__ffi(c_arg__iri);
    return handle_result(res);
}

SEXP savvy_validate_variable_name__impl(SEXP c_arg__name) {
    SEXP res = savvy_validate_variable_name__ffi(c_arg__name);
    return handle_result(res);
}

SEXP savvy_RArgument_from_constant_term__impl(SEXP c_arg__term, SEXP c_arg__list_expand) {
    SEXP res = savvy_RArgument_from_constant_term__ffi(c_arg__term, c_arg__list_expand);
    return handle_result(res);
}

SEXP savvy_RArgument_from_variable__impl(SEXP c_arg__name, SEXP c_arg__list_expand) {
    SEXP res = savvy_RArgument_from_variable__ffi(c_arg__name, c_arg__list_expand);
    return handle_result(res);
}

SEXP savvy_RConstantTerm_blank_node__impl(SEXP c_arg__id) {
    SEXP res = savvy_RConstantTerm_blank_node__ffi(c_arg__id);
    return handle_result(res);
}

SEXP savvy_RConstantTerm_iri__impl(SEXP c_arg__iri) {
    SEXP res = savvy_RConstantTerm_iri__ffi(c_arg__iri);
    return handle_result(res);
}

SEXP savvy_RConstantTerm_literal__impl(SEXP c_arg__value, SEXP c_arg__datatype_iri, SEXP c_arg__language) {
    SEXP res = savvy_RConstantTerm_literal__ffi(c_arg__value, c_arg__datatype_iri, c_arg__language);
    return handle_result(res);
}

SEXP savvy_RConstantTerm_none__impl(void) {
    SEXP res = savvy_RConstantTerm_none__ffi();
    return handle_result(res);
}

SEXP savvy_RGroundTerm_iri__impl(SEXP c_arg__iri) {
    SEXP res = savvy_RGroundTerm_iri__ffi(c_arg__iri);
    return handle_result(res);
}

SEXP savvy_RGroundTerm_literal__impl(SEXP c_arg__value, SEXP c_arg__datatype_iri, SEXP c_arg__language) {
    SEXP res = savvy_RGroundTerm_literal__ffi(c_arg__value, c_arg__datatype_iri, c_arg__language);
    return handle_result(res);
}

SEXP savvy_RInstance_iri__impl(SEXP self__) {
    SEXP res = savvy_RInstance_iri__ffi(self__);
    return handle_result(res);
}

SEXP savvy_RInstance_new__impl(SEXP c_arg__template_iri, SEXP c_arg__arguments, SEXP c_arg__list_expander) {
    SEXP res = savvy_RInstance_new__ffi(c_arg__template_iri, c_arg__arguments, c_arg__list_expander);
    return handle_result(res);
}

SEXP savvy_RModel_add_graph__impl(SEXP self__, SEXP c_arg__other, SEXP c_arg__source_graph, SEXP c_arg__target_graph) {
    SEXP res = savvy_RModel_add_graph__ffi(self__, c_arg__other, c_arg__source_graph, c_arg__target_graph);
    return handle_result(res);
}

SEXP savvy_RModel_add_prefixes__impl(SEXP self__, SEXP c_arg__prefixes) {
    SEXP res = savvy_RModel_add_prefixes__ffi(self__, c_arg__prefixes);
    return handle_result(res);
}

SEXP savvy_RModel_add_template__impl(SEXP self__, SEXP c_arg__template) {
    SEXP res = savvy_RModel_add_template__ffi(self__, c_arg__template);
    return handle_result(res);
}

SEXP savvy_RModel_add_template_string__impl(SEXP self__, SEXP c_arg__doc) {
    SEXP res = savvy_RModel_add_template_string__ffi(self__, c_arg__doc);
    return handle_result(res);
}

SEXP savvy_RModel_compact__impl(SEXP self__) {
    SEXP res = savvy_RModel_compact__ffi(self__);
    return handle_result(res);
}

SEXP savvy_RModel_create_index__impl(SEXP self__) {
    SEXP res = savvy_RModel_create_index__ffi(self__);
    return handle_result(res);
}

SEXP savvy_RModel_deserialize__impl(SEXP c_arg__path, SEXP c_arg__storage_folder) {
    SEXP res = savvy_RModel_deserialize__ffi(c_arg__path, c_arg__storage_folder);
    return handle_result(res);
}

SEXP savvy_RModel_detach_graph__impl(SEXP self__, SEXP c_arg__preserve_name, SEXP c_arg__graph) {
    SEXP res = savvy_RModel_detach_graph__ffi(self__, c_arg__preserve_name, c_arg__graph);
    return handle_result(res);
}

SEXP savvy_RModel_infer_rdfs__impl(SEXP self__, SEXP c_arg__graph) {
    SEXP res = savvy_RModel_infer_rdfs__ffi(self__, c_arg__graph);
    return handle_result(res);
}

SEXP savvy_RModel_insert__impl(SEXP self__, SEXP c_arg__query, SEXP c_arg__include_transient, SEXP c_arg__transient, SEXP c_arg__source_graph, SEXP c_arg__target_graph) {
    SEXP res = savvy_RModel_insert__ffi(self__, c_arg__query, c_arg__include_transient, c_arg__transient, c_arg__source_graph, c_arg__target_graph);
    return handle_result(res);
}

SEXP savvy_RModel_map__impl(SEXP self__, SEXP c_arg__template_iri, SEXP c_arg__stream_ptr, SEXP c_arg__graph, SEXP c_arg__validate_iris) {
    SEXP res = savvy_RModel_map__ffi(self__, c_arg__template_iri, c_arg__stream_ptr, c_arg__graph, c_arg__validate_iris);
    return handle_result(res);
}

SEXP savvy_RModel_map_df__impl(SEXP self__, SEXP c_arg__stream_ptr, SEXP c_arg__graph, SEXP c_arg__uuid_namespace) {
    SEXP res = savvy_RModel_map_df__ffi(self__, c_arg__stream_ptr, c_arg__graph, c_arg__uuid_namespace);
    return handle_result(res);
}

SEXP savvy_RModel_map_json__impl(SEXP self__, SEXP c_arg__path, SEXP c_arg__transient, SEXP c_arg__graph, SEXP c_arg__uuid_namespace) {
    SEXP res = savvy_RModel_map_json__ffi(self__, c_arg__path, c_arg__transient, c_arg__graph, c_arg__uuid_namespace);
    return handle_result(res);
}

SEXP savvy_RModel_map_json_string__impl(SEXP self__, SEXP c_arg__json, SEXP c_arg__transient, SEXP c_arg__graph, SEXP c_arg__uuid_namespace) {
    SEXP res = savvy_RModel_map_json_string__ffi(self__, c_arg__json, c_arg__transient, c_arg__graph, c_arg__uuid_namespace);
    return handle_result(res);
}

SEXP savvy_RModel_map_no_data__impl(SEXP self__, SEXP c_arg__template_iri, SEXP c_arg__graph, SEXP c_arg__validate_iris) {
    SEXP res = savvy_RModel_map_no_data__ffi(self__, c_arg__template_iri, c_arg__graph, c_arg__validate_iris);
    return handle_result(res);
}

SEXP savvy_RModel_map_triples__impl(SEXP self__, SEXP c_arg__stream_ptr, SEXP c_arg__predicate, SEXP c_arg__graph, SEXP c_arg__validate_iris) {
    SEXP res = savvy_RModel_map_triples__ffi(self__, c_arg__stream_ptr, c_arg__predicate, c_arg__graph, c_arg__validate_iris);
    return handle_result(res);
}

SEXP savvy_RModel_map_xml__impl(SEXP self__, SEXP c_arg__path, SEXP c_arg__transient, SEXP c_arg__graph, SEXP c_arg__uuid_namespace) {
    SEXP res = savvy_RModel_map_xml__ffi(self__, c_arg__path, c_arg__transient, c_arg__graph, c_arg__uuid_namespace);
    return handle_result(res);
}

SEXP savvy_RModel_map_xml_string__impl(SEXP self__, SEXP c_arg__xml, SEXP c_arg__transient, SEXP c_arg__graph, SEXP c_arg__uuid_namespace) {
    SEXP res = savvy_RModel_map_xml_string__ffi(self__, c_arg__xml, c_arg__transient, c_arg__graph, c_arg__uuid_namespace);
    return handle_result(res);
}

SEXP savvy_RModel_new__impl(void) {
    SEXP res = savvy_RModel_new__ffi();
    return handle_result(res);
}

SEXP savvy_RModel_query__impl(SEXP self__, SEXP c_arg__sparql, SEXP c_arg__stream_ptr, SEXP c_arg__include_transient, SEXP c_arg__graph, SEXP c_arg__bindings) {
    SEXP res = savvy_RModel_query__ffi(self__, c_arg__sparql, c_arg__stream_ptr, c_arg__include_transient, c_arg__graph, c_arg__bindings);
    return handle_result(res);
}

SEXP savvy_RModel_read__impl(SEXP self__, SEXP c_arg__path, SEXP c_arg__format, SEXP c_arg__graph) {
    SEXP res = savvy_RModel_read__ffi(self__, c_arg__path, c_arg__format, c_arg__graph);
    return handle_result(res);
}

SEXP savvy_RModel_reads__impl(SEXP self__, SEXP c_arg__s, SEXP c_arg__format, SEXP c_arg__graph) {
    SEXP res = savvy_RModel_reads__ffi(self__, c_arg__s, c_arg__format, c_arg__graph);
    return handle_result(res);
}

SEXP savvy_RModel_serialize__impl(SEXP self__, SEXP c_arg__path) {
    SEXP res = savvy_RModel_serialize__ffi(self__, c_arg__path);
    return handle_result(res);
}

SEXP savvy_RModel_size__impl(SEXP self__, SEXP c_arg__graph) {
    SEXP res = savvy_RModel_size__ffi(self__, c_arg__graph);
    return handle_result(res);
}

SEXP savvy_RModel_truncate_graph__impl(SEXP self__, SEXP c_arg__graph) {
    SEXP res = savvy_RModel_truncate_graph__ffi(self__, c_arg__graph);
    return handle_result(res);
}

SEXP savvy_RModel_update__impl(SEXP self__, SEXP c_arg__update, SEXP c_arg__include_transient, SEXP c_arg__graph) {
    SEXP res = savvy_RModel_update__ffi(self__, c_arg__update, c_arg__include_transient, c_arg__graph);
    return handle_result(res);
}

SEXP savvy_RModel_write__impl(SEXP self__, SEXP c_arg__path, SEXP c_arg__format, SEXP c_arg__graph) {
    SEXP res = savvy_RModel_write__ffi(self__, c_arg__path, c_arg__format, c_arg__graph);
    return handle_result(res);
}

SEXP savvy_RModel_writes__impl(SEXP self__, SEXP c_arg__format, SEXP c_arg__graph) {
    SEXP res = savvy_RModel_writes__ffi(self__, c_arg__format, c_arg__graph);
    return handle_result(res);
}

SEXP savvy_RParameter_new__impl(SEXP c_arg__variable_name, SEXP c_arg__optional, SEXP c_arg__allow_blank, SEXP c_arg__rdf_type_iri, SEXP c_arg__default_value) {
    SEXP res = savvy_RParameter_new__ffi(c_arg__variable_name, c_arg__optional, c_arg__allow_blank, c_arg__rdf_type_iri, c_arg__default_value);
    return handle_result(res);
}

SEXP savvy_RTemplate_instance__impl(SEXP self__, SEXP c_arg__arguments, SEXP c_arg__list_expander) {
    SEXP res = savvy_RTemplate_instance__ffi(self__, c_arg__arguments, c_arg__list_expander);
    return handle_result(res);
}

SEXP savvy_RTemplate_iri__impl(SEXP self__) {
    SEXP res = savvy_RTemplate_iri__ffi(self__);
    return handle_result(res);
}

SEXP savvy_RTemplate_new__impl(SEXP c_arg__iri, SEXP c_arg__parameters, SEXP c_arg__instances) {
    SEXP res = savvy_RTemplate_new__ffi(c_arg__iri, c_arg__parameters, c_arg__instances);
    return handle_result(res);
}

SEXP savvy_RTemplate_print_string__impl(SEXP self__) {
    SEXP res = savvy_RTemplate_print_string__ffi(self__);
    return handle_result(res);
}


static const R_CallMethodDef CallEntries[] = {
    {"savvy_export_test_series__impl", (DL_FUNC) &savvy_export_test_series__impl, 1},
    {"savvy_export_test_solution_mappings__impl", (DL_FUNC) &savvy_export_test_solution_mappings__impl, 1},
    {"savvy_import_test_series__impl", (DL_FUNC) &savvy_import_test_series__impl, 1},
    {"savvy_import_test_solution_mappings__impl", (DL_FUNC) &savvy_import_test_solution_mappings__impl, 2},
    {"savvy_make_triple__impl", (DL_FUNC) &savvy_make_triple__impl, 4},
    {"savvy_validate_blank_node_id__impl", (DL_FUNC) &savvy_validate_blank_node_id__impl, 1},
    {"savvy_validate_iri__impl", (DL_FUNC) &savvy_validate_iri__impl, 1},
    {"savvy_validate_variable_name__impl", (DL_FUNC) &savvy_validate_variable_name__impl, 1},
    {"savvy_RArgument_from_constant_term__impl", (DL_FUNC) &savvy_RArgument_from_constant_term__impl, 2},
    {"savvy_RArgument_from_variable__impl", (DL_FUNC) &savvy_RArgument_from_variable__impl, 2},
    {"savvy_RConstantTerm_blank_node__impl", (DL_FUNC) &savvy_RConstantTerm_blank_node__impl, 1},
    {"savvy_RConstantTerm_iri__impl", (DL_FUNC) &savvy_RConstantTerm_iri__impl, 1},
    {"savvy_RConstantTerm_literal__impl", (DL_FUNC) &savvy_RConstantTerm_literal__impl, 3},
    {"savvy_RConstantTerm_none__impl", (DL_FUNC) &savvy_RConstantTerm_none__impl, 0},
    {"savvy_RGroundTerm_iri__impl", (DL_FUNC) &savvy_RGroundTerm_iri__impl, 1},
    {"savvy_RGroundTerm_literal__impl", (DL_FUNC) &savvy_RGroundTerm_literal__impl, 3},
    {"savvy_RInstance_iri__impl", (DL_FUNC) &savvy_RInstance_iri__impl, 1},
    {"savvy_RInstance_new__impl", (DL_FUNC) &savvy_RInstance_new__impl, 3},
    {"savvy_RModel_add_graph__impl", (DL_FUNC) &savvy_RModel_add_graph__impl, 4},
    {"savvy_RModel_add_prefixes__impl", (DL_FUNC) &savvy_RModel_add_prefixes__impl, 2},
    {"savvy_RModel_add_template__impl", (DL_FUNC) &savvy_RModel_add_template__impl, 2},
    {"savvy_RModel_add_template_string__impl", (DL_FUNC) &savvy_RModel_add_template_string__impl, 2},
    {"savvy_RModel_compact__impl", (DL_FUNC) &savvy_RModel_compact__impl, 1},
    {"savvy_RModel_create_index__impl", (DL_FUNC) &savvy_RModel_create_index__impl, 1},
    {"savvy_RModel_deserialize__impl", (DL_FUNC) &savvy_RModel_deserialize__impl, 2},
    {"savvy_RModel_detach_graph__impl", (DL_FUNC) &savvy_RModel_detach_graph__impl, 3},
    {"savvy_RModel_infer_rdfs__impl", (DL_FUNC) &savvy_RModel_infer_rdfs__impl, 2},
    {"savvy_RModel_insert__impl", (DL_FUNC) &savvy_RModel_insert__impl, 6},
    {"savvy_RModel_map__impl", (DL_FUNC) &savvy_RModel_map__impl, 5},
    {"savvy_RModel_map_df__impl", (DL_FUNC) &savvy_RModel_map_df__impl, 4},
    {"savvy_RModel_map_json__impl", (DL_FUNC) &savvy_RModel_map_json__impl, 5},
    {"savvy_RModel_map_json_string__impl", (DL_FUNC) &savvy_RModel_map_json_string__impl, 5},
    {"savvy_RModel_map_no_data__impl", (DL_FUNC) &savvy_RModel_map_no_data__impl, 4},
    {"savvy_RModel_map_triples__impl", (DL_FUNC) &savvy_RModel_map_triples__impl, 5},
    {"savvy_RModel_map_xml__impl", (DL_FUNC) &savvy_RModel_map_xml__impl, 5},
    {"savvy_RModel_map_xml_string__impl", (DL_FUNC) &savvy_RModel_map_xml_string__impl, 5},
    {"savvy_RModel_new__impl", (DL_FUNC) &savvy_RModel_new__impl, 0},
    {"savvy_RModel_query__impl", (DL_FUNC) &savvy_RModel_query__impl, 6},
    {"savvy_RModel_read__impl", (DL_FUNC) &savvy_RModel_read__impl, 4},
    {"savvy_RModel_reads__impl", (DL_FUNC) &savvy_RModel_reads__impl, 4},
    {"savvy_RModel_serialize__impl", (DL_FUNC) &savvy_RModel_serialize__impl, 2},
    {"savvy_RModel_size__impl", (DL_FUNC) &savvy_RModel_size__impl, 2},
    {"savvy_RModel_truncate_graph__impl", (DL_FUNC) &savvy_RModel_truncate_graph__impl, 2},
    {"savvy_RModel_update__impl", (DL_FUNC) &savvy_RModel_update__impl, 4},
    {"savvy_RModel_write__impl", (DL_FUNC) &savvy_RModel_write__impl, 4},
    {"savvy_RModel_writes__impl", (DL_FUNC) &savvy_RModel_writes__impl, 3},
    {"savvy_RParameter_new__impl", (DL_FUNC) &savvy_RParameter_new__impl, 5},
    {"savvy_RTemplate_instance__impl", (DL_FUNC) &savvy_RTemplate_instance__impl, 3},
    {"savvy_RTemplate_iri__impl", (DL_FUNC) &savvy_RTemplate_iri__impl, 1},
    {"savvy_RTemplate_new__impl", (DL_FUNC) &savvy_RTemplate_new__impl, 3},
    {"savvy_RTemplate_print_string__impl", (DL_FUNC) &savvy_RTemplate_print_string__impl, 1},
    {NULL, NULL, 0}
};

void R_init_maplibr(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);

    // Functions for initialization, if any.

}
