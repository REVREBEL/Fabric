[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared Query = let
  Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/costar", [ApiVersion = 15]),
  #"Filtered hidden files" = Table.SelectRows(Source, each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Renamed columns" = Table.RenameColumns(#"Invoke custom function", {{"Name", "Source.Name"}}),
  #"Removed other columns" = Table.SelectColumns(#"Renamed columns", {"Source.Name", "Transform file"}),
  #"Removed errors" = Table.RemoveRowsWithErrors(#"Removed other columns", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed errors", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Tab 4 - Weekly Performance at a Glance - My Property vs. Competitive Set - Performance Set", type text}, {"Column4", type text}, {"Column6", type number}, {"Column9", type number}, {"Column12", type number}, {"Column15", type number}, {"Column18", type number}, {"Column21", type number}, {"Column24", type number}, {"Column27", type number}, {"Column28", type text}})
in
  #"Changed column type";
shared #"Sample file" = let
  Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/costar", [ApiVersion = 15]),
  #"Filtered hidden files" = Table.SelectRows(Source, each [Attributes]?[Hidden]? <> true),
  Navigation = #"Filtered hidden files"{[Name = "Weekly STAR_PopulusSeattle-20250427-USD-E-live.xlsx", #"Folder Path" = "https://revrebelhub.sharepoint.com/sites/costar/Shared Documents/20250427/"]}[Content]
in
  Navigation;
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Excel.Workbook(Parameter, null, true),
  Navigation = Source{[Item = "Glance_1", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Excel.Workbook(Parameter, null, true),
  Navigation = Source{[Item = "Glance_1", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true])
in
  #"Promoted headers";
