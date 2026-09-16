SEXP savvy_export_test_series__ffi(SEXP c_arg__stream_ptr);
SEXP savvy_export_test_solution_mappings__ffi(SEXP c_arg__stream_ptr);
SEXP savvy_import_test_series__ffi(SEXP c_arg__stream_ptr);
SEXP savvy_import_test_solution_mappings__ffi(SEXP c_arg__stream_ptr, SEXP c_arg__rdf_node_types_json);
SEXP savvy_make_triple__ffi(SEXP c_arg__subject, SEXP c_arg__predicate, SEXP c_arg__object, SEXP c_arg__list_expander);
SEXP savvy_validate_blank_node_id__ffi(SEXP c_arg__id);
SEXP savvy_validate_iri__ffi(SEXP c_arg__iri);
SEXP savvy_validate_variable_name__ffi(SEXP c_arg__name);

// methods and associated functions for RArgument
SEXP savvy_RArgument_from_constant_term__ffi(SEXP c_arg__term, SEXP c_arg__list_expand);
SEXP savvy_RArgument_from_variable__ffi(SEXP c_arg__name, SEXP c_arg__list_expand);

// methods and associated functions for RConstantTerm
SEXP savvy_RConstantTerm_blank_node__ffi(SEXP c_arg__id);
SEXP savvy_RConstantTerm_iri__ffi(SEXP c_arg__iri);
SEXP savvy_RConstantTerm_literal__ffi(SEXP c_arg__value, SEXP c_arg__datatype_iri, SEXP c_arg__language);
SEXP savvy_RConstantTerm_none__ffi(void);

// methods and associated functions for RGroundTerm
SEXP savvy_RGroundTerm_iri__ffi(SEXP c_arg__iri);
SEXP savvy_RGroundTerm_literal__ffi(SEXP c_arg__value, SEXP c_arg__datatype_iri, SEXP c_arg__language);

// methods and associated functions for RInstance
SEXP savvy_RInstance_iri__ffi(SEXP self__);
SEXP savvy_RInstance_new__ffi(SEXP c_arg__template_iri, SEXP c_arg__arguments, SEXP c_arg__list_expander);

// methods and associated functions for RModel
SEXP savvy_RModel_add_graph__ffi(SEXP self__, SEXP c_arg__other, SEXP c_arg__source_graph, SEXP c_arg__target_graph);
SEXP savvy_RModel_add_prefixes__ffi(SEXP self__, SEXP c_arg__prefixes);
SEXP savvy_RModel_add_template__ffi(SEXP self__, SEXP c_arg__template);
SEXP savvy_RModel_add_template_string__ffi(SEXP self__, SEXP c_arg__doc);
SEXP savvy_RModel_compact__ffi(SEXP self__);
SEXP savvy_RModel_create_index__ffi(SEXP self__);
SEXP savvy_RModel_deserialize__ffi(SEXP c_arg__path, SEXP c_arg__storage_folder);
SEXP savvy_RModel_detach_graph__ffi(SEXP self__, SEXP c_arg__preserve_name, SEXP c_arg__graph);
SEXP savvy_RModel_infer_rdfs__ffi(SEXP self__, SEXP c_arg__graph);
SEXP savvy_RModel_insert__ffi(SEXP self__, SEXP c_arg__query, SEXP c_arg__include_transient, SEXP c_arg__transient, SEXP c_arg__source_graph, SEXP c_arg__target_graph);
SEXP savvy_RModel_map__ffi(SEXP self__, SEXP c_arg__template_iri, SEXP c_arg__stream_ptr, SEXP c_arg__graph, SEXP c_arg__validate_iris);
SEXP savvy_RModel_map_df__ffi(SEXP self__, SEXP c_arg__stream_ptr, SEXP c_arg__graph, SEXP c_arg__uuid_namespace);
SEXP savvy_RModel_map_json__ffi(SEXP self__, SEXP c_arg__path, SEXP c_arg__transient, SEXP c_arg__graph, SEXP c_arg__uuid_namespace);
SEXP savvy_RModel_map_json_string__ffi(SEXP self__, SEXP c_arg__json, SEXP c_arg__transient, SEXP c_arg__graph, SEXP c_arg__uuid_namespace);
SEXP savvy_RModel_map_no_data__ffi(SEXP self__, SEXP c_arg__template_iri, SEXP c_arg__graph, SEXP c_arg__validate_iris);
SEXP savvy_RModel_map_triples__ffi(SEXP self__, SEXP c_arg__stream_ptr, SEXP c_arg__predicate, SEXP c_arg__graph, SEXP c_arg__validate_iris);
SEXP savvy_RModel_map_xml__ffi(SEXP self__, SEXP c_arg__path, SEXP c_arg__transient, SEXP c_arg__graph, SEXP c_arg__uuid_namespace);
SEXP savvy_RModel_map_xml_string__ffi(SEXP self__, SEXP c_arg__xml, SEXP c_arg__transient, SEXP c_arg__graph, SEXP c_arg__uuid_namespace);
SEXP savvy_RModel_new__ffi(void);
SEXP savvy_RModel_query__ffi(SEXP self__, SEXP c_arg__sparql, SEXP c_arg__stream_ptr, SEXP c_arg__include_transient, SEXP c_arg__graph, SEXP c_arg__bindings);
SEXP savvy_RModel_read__ffi(SEXP self__, SEXP c_arg__path, SEXP c_arg__format, SEXP c_arg__graph);
SEXP savvy_RModel_reads__ffi(SEXP self__, SEXP c_arg__s, SEXP c_arg__format, SEXP c_arg__graph);
SEXP savvy_RModel_serialize__ffi(SEXP self__, SEXP c_arg__path);
SEXP savvy_RModel_size__ffi(SEXP self__, SEXP c_arg__graph);
SEXP savvy_RModel_truncate_graph__ffi(SEXP self__, SEXP c_arg__graph);
SEXP savvy_RModel_update__ffi(SEXP self__, SEXP c_arg__update, SEXP c_arg__include_transient, SEXP c_arg__graph);
SEXP savvy_RModel_write__ffi(SEXP self__, SEXP c_arg__path, SEXP c_arg__format, SEXP c_arg__graph);
SEXP savvy_RModel_writes__ffi(SEXP self__, SEXP c_arg__format, SEXP c_arg__graph);

// methods and associated functions for RParameter
SEXP savvy_RParameter_new__ffi(SEXP c_arg__variable_name, SEXP c_arg__optional, SEXP c_arg__allow_blank, SEXP c_arg__rdf_type_iri, SEXP c_arg__default_value);

// methods and associated functions for RTemplate
SEXP savvy_RTemplate_instance__ffi(SEXP self__, SEXP c_arg__arguments, SEXP c_arg__list_expander);
SEXP savvy_RTemplate_iri__ffi(SEXP self__);
SEXP savvy_RTemplate_new__ffi(SEXP c_arg__iri, SEXP c_arg__parameters, SEXP c_arg__instances);
SEXP savvy_RTemplate_print_string__ffi(SEXP self__);
