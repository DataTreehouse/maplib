import logging

from maplib import Model

logger = logging.getLogger(__name__)


def infer_fully_inherited_instance_declaration(m: Model):
    logger.info("Start computing inherited instance declaration")

    logger.info("Start first level base query")
    first_level_base = """
        PREFIX ua:<http://opcfoundation.org/UA/>
        PREFIX uamapper:<https://github.com/DataTreehouse/uamapper#>
        CONSTRUCT { 
            ?type uamapper:baseFiid ?node .
            ?type uamapper:fiid ?node . 
            ?node uamapper:path ?browse_name . 
        }  WHERE {
            ?ns uamapper:typeNamespace true .
            ?type ua:Namespace ?ns .
            ?type ua:NodeClass ?node_class .
            ?type ?ref ?node .
            ua:HierarchicalReferences ua:HasSubtype+ ?ref .
            ?node ua:BrowseName ?browse_name .
            FILTER(
                ?node_class in (ua:UAObjectType, ua:UAVariableType) && 
                !STRSTARTS(?browse_name, "<") &&
                ?ref != ua:HasSubtype 
            )
        }"""
    m.insert(first_level_base)
    logger.info("Finish first level base query")

    logger.info("Start fiid count query")
    fiid_count = """
    PREFIX ua:<http://opcfoundation.org/UA/>
    PREFIX uamapper:<https://github.com/DataTreehouse/uamapper#>
    SELECT ?type ?node WHERE {
        ?type uamapper:fiid ?node .
    }
    """
    last_count = m.query(fiid_count).height
    finished = False
    logger.info("Finish fiid count query")

    first_level_recursion = """
            PREFIX ua:<http://opcfoundation.org/UA/>
            PREFIX uamapper:<https://github.com/DataTreehouse/uamapper#>
            CONSTRUCT { 
                ?type uamapper:baseFiid ?new_node .
                ?type uamapper:fiid ?new_node .
                ?new_node uamapper:path ?new_path . 
            }  WHERE {
                ?type uamapper:fiid ?node .
                ?node uamapper:path ?path .
                ?node ?ref ?new_node .
                ua:HierarchicalReferences ua:HasSubtype+ ?ref .
                ?new_node ua:BrowseName ?browse_name .
                BIND(CONCAT(?path, "/", ?browse_name) AS ?new_path)
                FILTER(
                    !STRSTARTS(?browse_name, "<") &&
                    ?ref != ua:HasSubtype 
                )
            }"""
    i = 0
    while not finished:
        logger.info(f"Start recursive instance declarations iteration {i}")
        m.insert(first_level_recursion)
        new_count = m.query(fiid_count).height
        logger.info(f"Last count was: {last_count}, new count is: {new_count}")
        finished = last_count == new_count
        last_count = new_count
        logger.info(f"Finish recursive instance declarations iteration {i}")
        i = i + 1

    # Inheritance no override
    inherit_no_override_rule = """
        PREFIX ua:<http://opcfoundation.org/UA/>
        PREFIX uamapper:<https://github.com/DataTreehouse/uamapper#>
        CONSTRUCT { ?subtype uamapper:fiid ?node }  WHERE {
            ?type ua:HasSubtype ?subtype .
            ?type uamapper:fiid ?node .
            ?node uamapper:path ?path .
            FILTER(
                NOT EXISTS {
                    ?subtype uamapper:path ?path .
                }
            )
        }"""

    finished = False
    while not finished:
        m.insert(inherit_no_override_rule)
        new_count = m.query(fiid_count).height
        logger.info(f"Last count was: {last_count}, new count is: {new_count}")
        finished = last_count == new_count
        last_count = new_count

    logger.info("Finished computing inheritance declaration")

    return m


def infer_browse_path_instances(m: Model) -> Model:
    logger.info("Start infer browse path instances")
    instances_count_query = """
        SELECT ?node ?type WHERE {
            ?node a ?type
        }
    """

    logger.info(f"Start infer browse path instances helper")
    subtyped_rel_query = """
    PREFIX ua:<http://opcfoundation.org/UA/>
    PREFIX uamapper:<https://github.com/DataTreehouse/uamapper#>
    SELECT DISTINCT ?parent_reference_type ?child_reference_type WHERE {
        ua:HierarchicalReferences ua:HasSubtype+ ?parent_reference_type .
        ?parent_reference_type ua:HasSubtype* ?child_reference_type .
    }
    """
    df = m.query(subtyped_rel_query)
    parent_child_list = [e for e in df.iter_rows(named=True)]
    # logger.info(parent_child_list)

    logger.info(f"Finish infer browse path instances helper")

    # We need to work around a memory issue here
    # Possibly this should be built into maplib
    browse_path_instances_query = """
        PREFIX ua:<http://opcfoundation.org/UA/>
        PREFIX uamapper:<https://github.com/DataTreehouse/uamapper#>
        CONSTRUCT { ?instance_child a ?type_child } WHERE {
            ?parent uamapper:fiid ?type_child .
            ?parent {parent_reference_type} ?type_child .
            ?instance a ?parent .
            ?instance {child_reference_type} ?instance_child .
            ?type_child ua:BrowseName ?browse_name .
            ?instance_child ua:BrowseName ?browse_name .
        }"""

    finished = False
    last_count = m.query(instances_count_query).height
    i = 0
    while not finished:
        logger.info(f"Iteration {i} for browse path instances")
        for e in parent_child_list:
            m.insert(
                browse_path_instances_query.replace(
                    "{parent_reference_type}", e["parent_reference_type"]
                ).replace("{child_reference_type}", e["child_reference_type"])
            )
        new_count = m.query(instances_count_query).height
        logger.info(
            f"Last instance count {last_count}," f" new instance count {new_count}"
        )
        finished = new_count == last_count
        last_count = new_count
        i += 1

    logger.info("Finish infer browse path instances")
    return m
