from typing import Dict
from polars import DataFrame
from polars.datatypes import N_INFER_DEFAULT
from polars.type_aliases import SchemaDefinition, FrameInitTypes, SchemaDict, Orientation

class SemanticDataFrame(DataFrame):
    """
    A Polars DataFrame but with an extra field rdf_datatypes containing the RDF data types of the columns.
    """
    def __init__(
            self,
            data: FrameInitTypes | None = None,
            schema: SchemaDefinition | None = None,
            *,
            schema_overrides: SchemaDict | None = None,
            orient: Orientation | None = None,
            infer_schema_length: int | None = N_INFER_DEFAULT,
            nan_to_null: bool = False,
            rdf_datatypes: Dict[str, str]
    ):
        """
        The signature of this method is from Polars, license can be found in the file ../../../LICENSING/POLARS_LICENSE
        SemanticDataFrames should be instantiated using the SemanticDataFrame.from_df()-method.
        This method mainly exists as a placeholder to make autocomplete work.
        """
        super().__init__(data, schema, schema_overrides=schema_overrides, orient=orient,
                         infer_schema_length=infer_schema_length, nan_to_null=nan_to_null)
        self.rdf_datatypes = rdf_datatypes

    @staticmethod
    def from_df(df: DataFrame, rdf_datatypes: Dict[str, str]) -> "SemanticDataFrame":
        """

        :param rdf_datatypes:
        :return:
        """
        df.__class__ = SemanticDataFrame
        df.init_rdf_datatypes(rdf_datatypes)
        return df

    def init_rdf_datatypes(self, map: Dict[str, str]):
        self.rdf_datatypes = map
