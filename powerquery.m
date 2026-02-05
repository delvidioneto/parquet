let
    // Precisa alterar o link do Sharepoint
    Files = SharePoint.Files("Link Sharepoint", [ApiVersion=15]),

    //Precisa alterar o caminho da pasta do sharepoint que esta a partição
    InFolder = Table.SelectRows(Files, each Text.Contains([Folder Path], "data/partitions")),

    OnlyParquet = Table.SelectRows(InFolder, each Text.EndsWith(Text.Lower([Name]), ".parquet")),
    Buffered = Table.TransformColumns(OnlyParquet, {{"Content", Binary.Buffer}}),
    WithTables = Table.AddColumn(Buffered, "Tbl", each Parquet.Document([Content])),
    Source = Table.Combine(WithTables[Tbl])
in
    Source