[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DemandData_Property = let
    //-- ✅ Get column names from sample file
    ColumnNames = Table.ColumnNames(#"Transform file"(#"Sample file")),
    //
    //-- ✅ Load files from SharePoint
    Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/demand", [ApiVersion = 15]),
    //
    //-- ✅ Filter for PMS folder and CSV files
    FilteredFiles = Table.SelectRows(Source, each 
        Text.Contains(Text.Lower([Folder Path]), "/_database/", Comparer.OrdinalIgnoreCase)
        and not Text.Contains(Text.Lower([Folder Path]), "/_schema_templates/", Comparer.OrdinalIgnoreCase)  
        and Text.Contains([Name], "Total_Hotel", Comparer.OrdinalIgnoreCase)
        //and Text.Contains([Name], "DTWDFH", Comparer.OrdinalIgnoreCase)
        and [Extension] = ".csv"
    ),
    //
    //-- ✅ Filter for PMS folder and CSV files
    FilteredHiddenFiles = Table.SelectRows(FilteredFiles, each [Attributes]?[Hidden]? <> true),
    InvokeCustomFunction = Table.AddColumn(FilteredHiddenFiles, "Transform file", each #"Transform file"([Content])),
    RenamedColumns = Table.RenameColumns(InvokeCustomFunction, {{"Name", "Source.Name"}}),
    RemovedOtherColumns = Table.SelectColumns(RenamedColumns, {"Source.Name", "Transform file"}),
    ExpandedTableColumn = Table.ExpandTableColumn(RemovedOtherColumns, "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
    //
    CleanedPercent = Table.TransformColumns(
        ExpandedTableColumn,
        {
            {"occ", each Number.FromText(Text.Replace(_, "%", "")) * 0.01, type number},
            {"cs_occ", each Number.FromText(Text.Replace(_, "%", "")) * 0.01, type number},
            {"occ_var_py", each Number.FromText(Text.Replace(_, "%", "")) * 0.01, type number},
            {"cs_occ_var_py", each Number.FromText(Text.Replace(_, "%", "")) * 0.01, type number},
            {"occ_index_var_py", each Number.FromText(Text.Replace(_, "%", "")) * 0.01, type number},
            {"adr_var_py", each Number.FromText(Text.Replace(_, "%", "")) * 0.01, type number},
            {"cs_adr_var_py", each Number.FromText(Text.Replace(_, "%", "")) * 0.01, type number}
        }
    ),
    RenamedMonthColumn = Table.RenameColumns(CleanedPercent, {"month", "month_year"}),
    TypedColumns = Table.TransformColumnTypes(RenamedMonthColumn, {
        {"segment", type text}, {"month_year", type text}, {"stay_date", type date},
        {"occ", type number}, {"occ_chg_pw", type number}, {"occ_var_py", type number},
        {"cs_occ", type number}, {"cs_occ_chg_pw", type number}, {"cs_occ_var_py", type number},
        {"occ_rank", type text}, {"adr_rank", type text}, {"adr_rank_py", type text}, {"revpar_rank", type text}, {"revpar_rank_py", type text},
        {"adr", type number}, {"adr_var_py", type number}, {"cs_adr", type number}, {"cs_adr_var_py", type number},
        {"occ_index", type number}, {"occ_index_chg_pw",type number}, {"occ_index_var_py", type number}
    }),
    RemovedColumns = Table.RemoveColumns(TypedColumns, {
        "occ_totals", "occ_cs_totals", "occ_index_total",
        "occ_rank_total", "adr_totals", "cd_adr_total",
        "adr_rank_total", "revpar_rank_total", "occ_var_py_total",
        "cs_occ_var_py_total", "occ_index_var_py_total", "adr_var_py_total",
        "cs_adr_var_py_total", "occ_chg_pw_total", "cs_occ_chg_pw_total",
        "occ_index_chg_pw_total", "adr_rank_py_total", "revpar_rank_py_total"
    }),
    //
     //-- ✅ Extract snapshot_date
    CreateSnapshotDate = Table.AddColumn(RemovedColumns, "snapshot_date", each try Text.Start([Source.Name], 8) otherwise null, type text),
    TrimmedSnapshotDate = Table.TransformColumns(CreateSnapshotDate, {{"snapshot_date", each Text.Trim(_), type text}}),
    ParsedSnapshotDate = Table.TransformColumns(TrimmedSnapshotDate, {{"snapshot_date", each try Date.FromText(_, [Format="yyyyMMdd"]) otherwise null, type date}}),
    //    
    //-- ✅ Extract property_code
    PropertyCodeExtraction = Table.TransformColumnTypes(Table.AddColumn(ParsedSnapshotDate, "property_code", each try Text.Middle([Source.Name], 9, 6) otherwise null), {{"property_code", type text}}),
    //    
    //-- ✅ Extract cs_set_id
    CompsetIdExtracted = Table.AddColumn(PropertyCodeExtraction, "cs_set_id", 
        each try let raw = Text.BetweenDelimiters([Source.Name], "IngestFile_", "_D360"), parts = Text.Split(raw, "_"), result = if List.Count(parts) >= 2 then parts{1} 
        else null in result otherwise null, type text),
    //
    // **** V1
    AddedDateMonthInfo = Table.AddColumn(CompsetIdExtracted, "month", each Date.Month(Date.From([stay_date])), Int64.Type),
    AddedDateYearInfo = Table.AddColumn(AddedDateMonthInfo, "cy", each Date.Year(Date.From([stay_date])), Int64.Type),
    AddedDateWeekdayInfo = Table.AddColumn(AddedDateYearInfo, "weekday", each Date.ToText(Date.From([stay_date]), "dddd"), type text),
    AddedDateDayofWeekInfo = Table.AddColumn(AddedDateWeekdayInfo, "dow", each Date.ToText(Date.From([stay_date]), "ddd"), type text),
    //
    //-- ✅ Added sent_to_big_query & date_sent_to_big_query
    AddedBQColumns = Table.AddColumn(Table.AddColumn(AddedDateDayofWeekInfo, "sent_to_big_query", each false, type logical), "date_sent_to_big_query", each null, type datetime),
    //
    //-- ✅ Remove rows with Totals from "month_year", Total from "stay_date", Date from "month_year" and Date from "stay_date"
    FilteredData = Table.SelectRows(AddedBQColumns, each 
        not Text.Contains(Text.From([month_year]), "Totals") and 
        not Text.Contains(Text.From([stay_date]), "Total") and 
        not Text.Contains(Text.From([month_year]), "Date") and 
        not Text.Contains(Text.From([stay_date]), "Date")
    ),
    //
    //-- ✅ Replace Values with Set1 or Set 2
    SafeSetIdTyped = Table.TransformColumnTypes(FilteredData, {{"cs_set_id", type text}}),
    ReplacementList = {{"D360", "Set1"}, {"Inge", "Set1"}, {"_D36", "Set1"}},
    ReplacedValues = List.Accumulate(
        ReplacementList, SafeSetIdTyped, (state, current) => Table.ReplaceValue(state, current{0}, current{1}, Replacer.ReplaceText, {"cs_set_id"})),
    //
    //-- ✅ Add cs_demand_id from the File Name 
    AddCsDemandId = Table.AddColumn(ReplacedValues,"cs_demand_id", each try Text.BetweenDelimiters([Source.Name], "IngestFile_", "_Set") otherwise null, type text),
    //
    //-- ✅ Add Demand Compset Lookup 
    AddedDemandCsLookup = Table.TransformColumnTypes(
        Table.AddColumn(AddCsDemandId, "demand_cs_lookup", each 
            let
                property_code_lookup = Text.Lower(Text.Remove([property_code], {"(", ")", "[", "]"})),
                cs_set_id_lookup = if Text.Trim([cs_set_id]) = "" or [cs_set_id] = null then null else Text.Lower(Text.Remove([cs_set_id], {"(", ")", "[", "]"})),
                cs_demand_id = if Text.Trim([cs_demand_id]) = "" or [cs_demand_id] = null then null else Text.Lower(Text.Remove([cs_demand_id], {"(", ")", "[", "]"}))
            in
                if cs_set_id_lookup = null or cs_demand_id = null then null
                else property_code_lookup & "_" & cs_set_id_lookup  & "_" & cs_demand_id
        ),
        {{"demand_cs_lookup", type text}}
    ),
    //
    // MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
    //-- ✅ Add Compset Data to Table from map_demand_data Google Sheet
    //
    MapDemandDataExists = try Table.HasColumns(AddedDemandCsLookup, "demand_cs_lookup") and Table.RowCount(SafeMapDemandData) > 0 otherwise false,
    //
    MergedDemandData = if MapDemandDataExists then 
        Table.NestedJoin(
            AddedDemandCsLookup, {"demand_cs_lookup"}, SafeMapDemandData, {"demand_cs_key"}, "map_demand_data", JoinKind.LeftOuter)
            else AddedDemandCsLookup,
    //
    ExpandedDemandData = 
        if MapDemandDataExists then 
            Table.TransformColumnTypes(
                Table.ExpandTableColumn(
                    MergedDemandData,
                    "map_demand_data", {"physical_capacity", "cs_physical_capacity"}),
                    {{"physical_capacity", Int64.Type}, {"cs_physical_capacity", Int64.Type}}
                )
        else 
        let
            existingCols = Table.ColumnNames(AddedDemandCsKey),
            columnsToConvert = List.Intersect({existingCols, {"physical_capacity", "cs_physical_capacity"}}),
            transformOps = List.Transform(columnsToConvert, each {_, each try Int64.From(_) otherwise 0, Int64.Type})
        in
            Table.TransformColumns(AddedDemandCsLookup, transformOps),
    //
    FinalOutputMergedDemandData = ExpandedDemandData,
    //
    // MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
    // ✅ Add Property Data to Table from map_property Google Sheet
    // 
    MapPropertyDataExists = try Table.RowCount(SafeMapProperty) > 0 otherwise false,
    //
    MergedPropertyData = if MapPropertyDataExists then 
        Table.NestedJoin(
            FinalOutputMergedDemandData, {"property_code"}, SafeMapProperty, {"property_code_key"}, "map_property",  JoinKind.LeftOuter ) 
            else  FinalOutputMergedDemandData,
    //
    ExpandedPropertyData = if MapPropertyDataExists then 
        Table.ExpandTableColumn(
            MergedPropertyData, "map_property", {"property_name", "property_shortname"}) 
            else FinalOutputMergedDemandData,
    //
    FinalOutputMergedPropertyData = ExpandedPropertyData,
    //
    // MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM    
    // ✅ Add Property Data to Table from map_property Google Sheet
    WithIngestedTimestamp = Table.AddColumn(FinalOutputMergedPropertyData, "ingested_timestamp", each DateTime.LocalNow(), type datetime),
    ColumnOrder = {
        "property_code", "property_name", "property_shortname", "cs_set_id", "physical_capacity", "cs_physical_capacity", 
        "stay_date", "month", "month_year", "cy", "weekday", "dow", 
        "occ", "occ_chg_pw", "occ_var_py",  
        "cs_occ", "cs_occ_chg_pw", "cs_occ_var_py",  
        "occ_index", "occ_index_chg_pw", "occ_index_var_py", "occ_rank",
        "adr", "adr_var_py", "cs_adr", "cs_adr_var_py", 
        "adr_rank", "adr_rank_py", "revpar_rank", "revpar_rank_py", 
        "cs_demand_id", "snapshot_date", "ingested_timestamp", "sent_to_big_query", "date_sent_to_big_query"
    },
    AvailableColumns = Table.ColumnNames(WithIngestedTimestamp),
    ReorderedColumnsList = List.Intersect({ColumnOrder, AvailableColumns}),
    ReorderedTable = Table.ReorderColumns(WithIngestedTimestamp, ReorderedColumnsList, MissingField.Ignore),
    FinalOutput = Table.RemoveColumns(ReorderedTable, {"Source.Name", "demand_cs_lookup", "demand_cs_key"}, MissingField.Ignore)
in
    FinalOutput;
shared #"Sample file" = let
  // -- ✅ Step 1: Load files from SharePoint & Filter Relevant Files
  Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/demand", [ApiVersion = 15]),
  FilteredFiles = Table.SelectRows(Source, each 
        Text.Contains(Text.Lower([Folder Path]), "/_database/_schema_templates/", Comparer.OrdinalIgnoreCase)  and 
        [Extension] = ".csv" and 
        ([Attributes]?[Hidden]? <> true)
    ),
  TemplateFile = Table.SelectRows(FilteredFiles, each Text.Contains([Name], "D360_T1_Total_Hotel_Schema.csv")),
  FirstFile = try TemplateFile{0}[Content] otherwise error "D360 Total Hotel Schema template file not found"
in
  FirstFile;
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
    // Try to read as UTF-8 comma-delimited first
    CsvUTF8 = try Csv.Document(Parameter, [Delimiter = ",", QuoteStyle = QuoteStyle.None]),

    // If that fails, fallback to UTF-16 tab-delimited
    Source = if CsvUTF8[HasError] then
                Csv.Document(Parameter, [Delimiter = "#(tab)", Encoding = 1200, QuoteStyle = QuoteStyle.None])
             else
                CsvUTF8[Value],

    // Promote headers
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),

    // Optional: Clean headers if needed
    CleanedHeaders = Table.SelectColumns(PromotedHeaders, {"column1", "column2", "column3"}, MissingField.Ignore),

    NormalizeHeader = (columnName as text) as text =>
        Text.Lower(Text.Replace(columnName, " ", "_")),

    RenamedHeaders = Table.TransformColumnNames(PromotedHeaders, NormalizeHeader),
    TypedColumns = Table.TransformColumnTypes(RenamedHeaders, {
      {"segment", type text},
      {"month", type text},
      {"stay_date", type datetime}
    })
in
    TypedColumns;
shared ColumnNames = let
  ColumnNames = Table.ColumnNames(#"Transform Sample file"),
  ConvertToTable = Table.FromValue(ColumnNames),
  TransformColumns = Table.TransformColumnTypes(ConvertToTable, {{"Value", type text}}),
  ReplaceErrors = Table.ReplaceErrorValues(TransformColumns, {{"Value", null}})
in
  ReplaceErrors;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
    // Try to read as UTF-8 comma-delimited first
    CsvUTF8 = try Csv.Document(Parameter, [Delimiter = ",", QuoteStyle = QuoteStyle.None]),

    // If that fails, fallback to UTF-16 tab-delimited
    Source = if CsvUTF8[HasError] then
                Csv.Document(Parameter, [Delimiter = "#(tab)", Encoding = 1200, QuoteStyle = QuoteStyle.None])
             else
                CsvUTF8[Value],

    // Promote headers
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),

    // Optional: Clean headers if needed
    CleanedHeaders = Table.SelectColumns(PromotedHeaders, {"column1", "column2", "column3"}, MissingField.Ignore),

    NormalizeHeader = (columnName as text) as text =>
        Text.Lower(Text.Replace(columnName, " ", "_")),

    RenamedHeaders = Table.TransformColumnNames(PromotedHeaders, NormalizeHeader),
    TypedColumns = Table.TransformColumnTypes(RenamedHeaders, {
      {"segment", type text},
      {"month", type text},
      {"stay_date", type datetime}
    })
in
    TypedColumns;
shared GetColumnNames = let
    Source = SharePoint.Files("https://aparium.sharepoint.com/sites/demand-reporting/", [ApiVersion = 15]),
    //
    // STEP 2: Filter for PMS folder and CSV files
    FilteredFiles = Table.SelectRows(Source, each 
        Text.Contains(Text.Lower([Folder Path]), "/shared documents/demand datastore/_database/_schema_templates/", Comparer.OrdinalIgnoreCase)
        and Text.Contains([Name], "Total_Hotel", Comparer.OrdinalIgnoreCase)
        and [Extension] = ".csv"
    ),
  FilteredHiddenFiles = Table.SelectRows(FilteredFiles, each [Attributes]?[Hidden]? <> true),
  SampleFile = Table.FirstN(FilteredFiles, 1){0},
  SampleContent = SampleFile[Content],
  TransformedSample = #"Transform file"(SampleContent),
  ColumnNames = Table.ColumnNames(TransformedSample),
  Quoted = List.Transform(ColumnNames, each """" & _ & ""","),
  OutputText = Text.Combine(Quoted, "#(cr)#(lf)")
//  ConvertToTable = Table.FromValue(OutputText)
in
  OutputText;
shared ReplaceColumnNames = let
    // STEP 1: Load files from SharePoint
    Source = SharePoint.Files("https://aparium.sharepoint.com/sites/demand-reporting/", [ApiVersion = 15]),
    //
    // STEP 2: Filter for PMS folder and CSV files
    FilteredFiles = Table.SelectRows(Source, each 
        Text.Contains(Text.Lower([Folder Path]), "/shared documents/demand datastore/_database/", Comparer.OrdinalIgnoreCase) 
        and Text.Contains([Name], "Total_Hotel", Comparer.OrdinalIgnoreCase)
        and Text.Contains([Name], "AEXHER", Comparer.OrdinalIgnoreCase)
        and [Extension] = ".csv"
    ),
  FilteredHiddenFiles = Table.SelectRows(FilteredFiles, each [Attributes]?[Hidden]? <> true),
  SampleFile = Table.FirstN(FilteredFiles, 1){0},
  SampleContent = SampleFile[Content],
  TransformedSample = #"Transform file"(SampleContent),
  ColumnNames = Table.ColumnNames(TransformedSample),
  RenamePairs = List.Transform(ColumnNames, each "    {""" & _ & """, """ & _ & """}"),
  RenameText = Text.Combine(RenamePairs, "," & "#(cr)#(lf)"),
  OutputText = "// 🚀 STEP 3: Rename Key Columns" & "#(cr)#(lf)" & "#(cr)#(lf)" & "RenamedColumns = Table.RenameColumns(FinalRemovedColumns, {" & "#(cr)#(lf)" & RenameText & "#(cr)#(lf)" & "}),"
//  ConvertToTable = Table.FromValue(OutputText)
in
  OutputText;
shared GetColumnNames_Table = let
  // Combine already-transformed tables
  GetTables = Table.Combine({map_compset_gsheet}),
  // Use the first actual data row/table
  SampleRow = Table.FirstN(GetTables, 1),
  // Convert to a table for schema analysis
  TransformedSample = SampleRow,
  ColumnSchema = Table.Schema(TransformedSample),
  ColumnNames = Table.ColumnNames(TransformedSample),
  Quoted = List.Transform(ColumnNames, each """" & _ & ""","),
  OutputText = Text.Combine(Quoted, "#(cr)#(lf)")
in
  OutputText;
shared GetColumnTypes = let
  Source = SharePoint.Files("https://aparium.sharepoint.com/sites/demand-reporting/", [ApiVersion = 15]),
  //
  // STEP 2: Filter for PMS folder and CSV files
  FilteredFiles = Table.SelectRows(Source, each 
      Text.Contains(Text.Lower([Folder Path]), "/shared documents/demand datastore/_database/_schema_templates/", Comparer.OrdinalIgnoreCase)
      and Text.Contains([Name], "Total_Hotel", Comparer.OrdinalIgnoreCase)
      and [Extension] = ".csv"
  ),
  FilteredHiddenFiles = Table.SelectRows(FilteredFiles, each [Attributes]?[Hidden]? <> true),
  SampleFile = Table.FirstN(FilteredFiles, 1){0},
  SampleContent = SampleFile[Content],
  TransformedSample = #"Transform file"(SampleContent),
  // Convert to a table for schema analysis
  ColumnSchema = Table.Schema(TransformedSample),
  TypePairs = List.Transform(
        Table.ToRecords(ColumnSchema), 
        each "    {""" & [Name] & """, type " & Text.Lower(Text.From([Kind])) & "}"
    ),
  CombinedTypeText = Text.Combine(TypePairs, "," & "#(cr)#(lf)"),
  OutputText = "ChangedColumnType = Table.TransformColumnTypes(RenamedColumnHeaders, {" & "#(cr)#(lf)" & "    //" & "#(cr)#(lf)" & CombinedTypeText & "#(cr)#(lf)" & "    //" & "#(cr)#(lf)" & "}),"
in
  OutputText;
shared GetColumnTypes_Table = let
  // Combine already-transformed tables
  GetTables = Table.Combine({map_demand_data_gsheet}),
  // Use the first actual data row/table
  SampleRow = Table.FirstN(GetTables, 1),
  // Convert to a table for schema analysis
  TransformedSample = SampleRow,
  ColumnSchema = Table.Schema(TransformedSample),
  TypePairs = List.Transform(
        Table.ToRecords(ColumnSchema), 
        each "    {""" & [Name] & """, type " & Text.Lower(Text.From([Kind])) & "}"
    ),
  CombinedTypeText = Text.Combine(TypePairs, "," & "#(cr)#(lf)"),
  OutputText = "ChangedColumnType = Table.TransformColumnTypes(RenamedColumnHeaders, {" & "#(cr)#(lf)" & "    //" & "#(cr)#(lf)" & CombinedTypeText & "#(cr)#(lf)" & "    //" & "#(cr)#(lf)" & "}),"
in
  OutputText;
shared map_property_gsheet = let
    Source = Csv.Document(
        Web.Contents("https://docs.google.com/spreadsheets/d/1JERzvRDvVbwfdEhFwF6JtwLLJ2md-IFFd5KLnS1ZRo8/export?format=csv&id=1JERzvRDvVbwfdEhFwF6JtwLLJ2md-IFFd5KLnS1ZRo8&gid=2001325599"),
        [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]
    ),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true])
in
    #"Promoted Headers";
shared SafeMapProperty = let
  ExpectedColumns = {
    "property_code_key",
    "pms_property_code",
    "property_name",
    "physical_capacity",
    "property_shortname",
    "crs_property_code"
  },
  //
  ActualColumns = try Table.ColumnNames(map_property_gsheet) otherwise {},
  SchemaValid = List.Intersect({ActualColumns, ExpectedColumns}) = ExpectedColumns,
  //
  SafeMapPropertyRaw = if SchemaValid 
      then map_property_gsheet 
      else #table(ExpectedColumns, {}),
  //
  TransformColumns = Table.TransformColumnTypes(SafeMapPropertyRaw, {
    {"property_code_key", type text},
    {"pms_property_code", type text},
    {"property_name", type text},
    {"physical_capacity", Int64.Type},
    {"property_shortname", type text},
    {"crs_property_code", type text}
  }),
  //
  ReplaceErrors = Table.ReplaceErrorValues(TransformColumns, {
    {"property_code_key", null},
    {"pms_property_code", null},
    {"property_name", null},
    {"physical_capacity", null},
    {"property_shortname", null},
    {"crs_property_code", null}
  })
in
  ReplaceErrors;
shared SafeMapPropertySchemaFallback = let
  EmptyMapPropertyTable = #table(
  //
    {
    //
    "property_code_key",
    "pms_property_code",
    "property_name",
    "physical_capacity",
    "property_shortname",
    "crs_property_code"
    //
    },
    {}
  ),
  //
  TransformColumns = Table.TransformColumnTypes(EmptyMapPropertyTable, {
  //
    {"property_code_key", type text},
    {"pms_property_code", type text},
    {"property_name", type text},
    {"physical_capacity", Int64.Type},
    {"property_shortname", type text},
    {"crs_property_code", type text}
  //
  }),
  //
  ReplaceErrors = Table.ReplaceErrorValues(TransformColumns, {
  //
    {"property_code_key", null},
    {"pms_property_code", null},
    {"property_name", null},
    {"physical_capacity", null},
    {"property_shortname", null},
    {"crs_property_code", null}
  //
  })
  //
in
  ReplaceErrors;
shared map_compset_gsheet = let
    Source = Csv.Document(
        Web.Contents("https://docs.google.com/spreadsheets/d/1JERzvRDvVbwfdEhFwF6JtwLLJ2md-IFFd5KLnS1ZRo8/export?format=csv&id=1JERzvRDvVbwfdEhFwF6JtwLLJ2md-IFFd5KLnS1ZRo8&gid=1191952642"),
        [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]
    ),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true])
in
    #"Promoted Headers";
shared SafeMapCompset = let
      ExpectedColumns = {
            "property_code_key",
            "cs_set_id",
            "cs_start_date",
            "cs_name",
            "cs_propername",
            "cs_shortname",
            "cs_brand",
            "cs_chain",
            "cs_chain_code",
            "cs_geo_code",
            "cs_physical_capacity_text",
            "cs_physical_capacity",
            "cs_capacity_pct",
            "cs_distance_text",
            "cs_phone",
            "cs_address"
      },
      //
      ActualColumns = try Table.ColumnNames(map_compset_gsheet) otherwise {},
      SchemaValid = List.Difference(ExpectedColumns, ActualColumns) = {},

      SafeMapCompsetRaw = if SchemaValid 
      then map_compset_gsheet 
      else #table(ExpectedColumns, {}),
      TransformColumns = Table.TransformColumnTypes(SafeMapCompsetRaw, {
      //
            {"property_code_key", type text},
            {"cs_set_id", type text},
            {"cs_start_date", type text},
            {"cs_name", type text},
            {"cs_propername", type text},
            {"cs_shortname", type text},
            {"cs_brand", type text},
            {"cs_chain", type text},
            {"cs_chain_code", type text},
            {"cs_geo_code", type text},
            {"cs_physical_capacity_text", type text},
            {"cs_physical_capacity", Int64.Type},
            {"cs_capacity_pct", type number},
            {"cs_distance_text", type text},
            {"cs_phone", type text},
            {"cs_address", type text}
      }),
      //
      ReplaceErrors = Table.ReplaceErrorValues(TransformColumns, {
            {"property_code_key", null},
            {"cs_set_id", null},
            {"cs_start_date", null},
            {"cs_name", null},
            {"cs_propername", null},
            {"cs_shortname", null},
            {"cs_brand", null},
            {"cs_chain", null},
            {"cs_chain_code", null},
            {"cs_geo_code", null},
            {"cs_physical_capacity_text", null},
            {"cs_physical_capacity", null},
            {"cs_capacity_pct", null},
            {"cs_distance_text", null},
            {"cs_phone", null},
            {"cs_address", null}
      })
in
      ReplaceErrors;
shared SafeMapCompsetSchemaFallback = let
  EmptyMapCompsetTable = #table(
  //
        {
        //
            "property_code_key",
            "cs_set_id",
            "cs_start_date",
            "cs_name",
            "cs_propername",
            "cs_shortname",
            "cs_brand",
            "cs_chain",
            "cs_chain_code",
            "cs_geo_code",
            "cs_physical_capacity_text",
            "cs_physical_capacity",
            "cs_capacity_pct",
            "cs_distance_text",
            "cs_phone",
            "cs_address"
      //
      },
      {}
    ),
  TransformColumns = Table.TransformColumnTypes(EmptyMapCompsetTable, {
  //
            {"property_code_key", type text},
            {"cs_set_id", type text},
            {"cs_start_date", type text},
            {"cs_name", type text},
            {"cs_propername", type text},
            {"cs_shortname", type text},
            {"cs_brand", type text},
            {"cs_chain", type text},
            {"cs_chain_code", type text},
            {"cs_geo_code", type text},
            {"cs_physical_capacity_text", type text},
            {"cs_physical_capacity", Int64.Type},
            {"cs_capacity_pct", type number},
            {"cs_distance_text", type text},
            {"cs_phone", type text},
            {"cs_address", type text}
//
  }),
  //
  ReplaceErrors = Table.ReplaceErrorValues(TransformColumns, {
  //
            {"property_code_key", null},
            {"cs_set_id", null},
            {"cs_start_date", null},
            {"cs_name", null},
            {"cs_propername", null},
            {"cs_shortname", null},
            {"cs_brand", null},
            {"cs_chain", null},
            {"cs_chain_code", null},
            {"cs_geo_code", null},
            {"cs_physical_capacity_text", null},
            {"cs_physical_capacity", null},
            {"cs_capacity_pct", null},
            {"cs_distance_text", null},
            {"cs_phone", null},
            {"cs_address", null}
  //
  })
in
  ReplaceErrors;
shared map_demand_data_gsheet = let
    Source = Csv.Document(
        Web.Contents("https://docs.google.com/spreadsheets/d/1JERzvRDvVbwfdEhFwF6JtwLLJ2md-IFFd5KLnS1ZRo8/export?format=csv&id=1JERzvRDvVbwfdEhFwF6JtwLLJ2md-IFFd5KLnS1ZRo8&gid=2141383019"),
        [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]
    ),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
  ChangedColumnTypeData = Table.TransformColumnTypes(#"Promoted Headers", {{"cs_start_date", type date}, {"demand_cs_key", type text}, {"property_code", type text}, {"cs_demand_id", type text}, {"cs_set_id", type text}, {"physical_capacity", Int64.Type}, {"cs_physical_capacity", Int64.Type}, {"cs_end_date", type text}})
in
    ChangedColumnTypeData;
shared SafeMapDemandData = let
      ExpectedColumns = {
            "demand_cs_key",
            "property_code",
            "cs_demand_id",
            "cs_set_id",
            "physical_capacity",
            "cs_physical_capacity",
            "cs_start_date",
            "cs_end_date"
      },
      //
      ActualColumns = try Table.ColumnNames(map_demand_data_gsheet) otherwise {},
      SchemaValid = List.Difference(ExpectedColumns, ActualColumns) = {},

      SafeMapDemandDataRaw = if SchemaValid 
      then map_demand_data_gsheet 
      else #table(ExpectedColumns, {}),
      //
      TransformColumns = Table.TransformColumnTypes(SafeMapDemandDataRaw, {
            {"demand_cs_key", type text},
            {"property_code", type text},
            {"cs_demand_id", type text},
            {"cs_set_id", type text},
            {"physical_capacity", type number},
            {"cs_physical_capacity", type number},
            {"cs_start_date", type date},
            {"cs_end_date", type text}
      }),
      //
      ReplaceErrors = Table.ReplaceErrorValues(TransformColumns, {
            {"demand_cs_key", null},
            {"property_code", null},
            {"cs_demand_id", null},
            {"cs_set_id", null},
            {"physical_capacity", type number},
            {"cs_physical_capacity", type number},
            {"cs_start_date", type date},
            {"cs_end_date", null}  
      })
in
      ReplaceErrors;
shared SafeMapDemandDataSchemaFallback = let
  EmptyMapDemandDataTable = #table(
  //
    {
    //
            "demand_cs_key",
            "property_code",
            "cs_demand_id",
            "cs_set_id",
            "physical_capacity",
            "cs_physical_capacity",
            "cs_start_date",
            "cs_end_date"
  //
  },
  {}
  ),
  TransformColumns = Table.TransformColumnTypes(EmptyMapDemandDataTable, {
  //
            {"demand_cs_key", type text},
            {"property_code", type text},
            {"cs_demand_id", type text},
            {"cs_set_id", type text},
            {"physical_capacity", type number},
            {"cs_physical_capacity", type number},
            {"cs_start_date", type date},
            {"cs_end_date", type text}
  //
  }),
  //
  ReplaceErrors = Table.ReplaceErrorValues(TransformColumns, {
  //
            {"demand_cs_key", null},
            {"property_code", null},
            {"cs_demand_id", null},
            {"cs_set_id", null},
            {"physical_capacity", type number},
            {"cs_physical_capacity", type number},
            {"cs_start_date", type date},
            {"cs_end_date", null} 
  //
  })
in
  ReplaceErrors;
shared DemandData_Property_Debug = let
    GetTables = Table.Combine({
      DemandData_Property
    }),

    ColumnOrder = {
        "property_code", "property_name", "property_shortname", "cs_set_id", "physical_capacity", "cs_physical_capacity", 
        "stay_date", "month", "month_year",  "cy", "weekday", "dow", 
        "segment", "occ", "occ_chg_pw", "occ_var_py",  
        "cs_occ", "cs_occ_chg_pw", "cs_occ_var_py",  
        "occ_index", "occ_index_chg_pw", "occ_index_var_py", "occ_rank",
        "adr", "adr_var_py", 
        "cs_adr", "cs_adr_var_py", 
        "adr_rank", "adr_rank_py",
        "revpar_rank", "revpar_rank_py", 
        "cs_demand_id", "snapshot_date", "ingested_timestamp", "sent_to_big_query", "date_sent_to_big_query"
    },

    AvailableColumns = Table.ColumnNames(GetTables),
    ReorderedColumnsList = List.Intersect({ColumnOrder, AvailableColumns}),
    ReorderedTable = Table.ReorderColumns(GetTables, ReorderedColumnsList),


    AddErrorDetail = Table.AddColumn(ReorderedTable, "ErrorColumns", each 
        List.Select(
            Record.FieldNames(_),
            (col) => try Value.Is(Record.Field(_, col), Value.Type(error 1)) otherwise false
        )
    ),
    FilteredWithErrors = Table.SelectRows(AddErrorDetail, each List.Count([ErrorColumns]) > 0),

    // 🚫 STEP 25: Remove unsupported columns before Warehouse write
    FinalOutput = ReorderedTable

in
    FinalOutput;
