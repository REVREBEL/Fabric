[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"Transform Sample file" = let
    Source = Excel.Workbook(Parameter, null, true),
    Navigation = Source{[Item = "Room Type", Kind = "Sheet"]}[Data],
    PromotedHeaders = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),
    //
    // 🧼 Normalize headers — lowercase + replace spaces with underscores
    NormalizeHeader = (columnName as text) as text =>
        Text.Lower(Text.Replace(columnName, " ", "_")),
    //
    RenamedHeaders = Table.TransformColumnNames(PromotedHeaders, NormalizeHeader),
    //
    RemovedColumns = Table.RemoveColumns(RenamedHeaders, {"property_name"}),
    //
    FinalOutput = RemovedColumns
in
    FinalOutput;
shared ColumnNames = let
    ColumnNames = Table.ColumnNames(#"Transform Sample file")
in
    ColumnNames;
shared #"Sample file" = let
  // -- ✅ Step 1: Load files from SharePoint & Filter Relevant Files
  Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/pace", [ApiVersion = 15]),
  FilteredFiles = Table.SelectRows(Source, each 
        Text.Contains(Text.Lower([Folder Path]), "/_schema_templates/", Comparer.OrdinalIgnoreCase)  and 
        [Extension] = ".xlsx" and 
        ([Attributes]?[Hidden]? <> true)
    ),
  TemplateFile = Table.SelectRows(FilteredFiles, each Text.Contains([Name], "PaceData_Schema.xlsx")),
  FirstFile = try TemplateFile{0}[Content] otherwise error "Pace Schema template file not found"
in
  FirstFile;
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform file" = (Parameter as binary) => let
    Source = Excel.Workbook(Parameter, null, true),
    Navigation = Source{[Item = "Business View", Kind = "Sheet"]}[Data],
    PromotedHeaders = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),

    // Step 1: Normalize headers
    NormalizeHeader = (columnName as text) as text =>
        let
            lower = Text.Lower(Text.Replace(columnName, " ", "_")),
            replaced = 
                if Text.StartsWith(lower, "my_forecast_") then
                    Text.Replace(lower, "my_forecast_", "property_forecast_")
                else if Text.StartsWith(lower, "user_forecast_") then
                    Text.Replace(lower, "user_forecast_", "property_forecast_")
                else if Text.StartsWith(lower, "user_projected_") then
                    Text.Replace(lower, "user_projected_", "property_forecast_")
                else
                    lower
        in
            replaced,

    RenamedHeaders = Table.TransformColumnNames(PromotedHeaders, NormalizeHeader),

    // Remove columnXX artifacts
    RemoveColumns = Table.RemoveColumns(RenamedHeaders, List.Select(Table.ColumnNames(RenamedHeaders), each Text.StartsWith(_, "column") and (try Number.From(Text.Middle(_, 6)) is number otherwise false))),
    RemovedPropertyNameColumn = Table.RemoveColumns(RemoveColumns, {"property_name"}),
    // Step 2: Coalesce column variants caused by expansion duplicates
    CoalesceColumns = (baseTable as table, baseColName as text) as table =>
        let
            allCols = Table.ColumnNames(baseTable),
            matches = List.Select(allCols, each _ = baseColName or Text.StartsWith(_, baseColName & "1")),
            withMerged = if List.Count(matches) > 1 then
                let
                tempCol = baseColName & "_merged",
                added = Table.AddColumn( baseTable, tempCol, 
                    each try List.First(List.RemoveNulls(Record.ToList(Record.SelectFields(_, matches))), null) otherwise null),
                removed = Table.RemoveColumns(added, matches),
                renamed = Table.RenameColumns(removed, {{tempCol, baseColName}})
                  
                in
                    renamed
            else
                baseTable
        in
            withMerged,

    // List the normalized columns you want to coalesce
    ColumnsToMerge = {
        "property_forecast_revenue_this_year",
        "property_forecast_revenue_actual_last_year"
    },
    // Apply the coalescing to clean up suffix duplicates (e.g. ...1, ...2)
    FinalOutput = List.Accumulate(ColumnsToMerge, RemovedPropertyNameColumn, (state, colName) => CoalesceColumns(state, colName))

in 
    FinalOutput;
shared map_property_gsheet = let
    Source = Csv.Document(
        Web.Contents("https://docs.google.com/spreadsheets/d/19r9FdllD9Mn3zZ4dr8Ss4S5Yq2xkaIV45RudibfPUUs/export?format=csv&id=19r9FdllD9Mn3zZ4dr8Ss4S5Yq2xkaIV45RudibfPUUs&gid=2058044192"),
        [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]
    ),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true])
in
    PromotedHeaders;
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
shared map_roomtype_gsheet = let
    Source = Csv.Document(
        Web.Contents("https://docs.google.com/spreadsheets/d/19r9FdllD9Mn3zZ4dr8Ss4S5Yq2xkaIV45RudibfPUUs/export?format=csv&id=19r9FdllD9Mn3zZ4dr8Ss4S5Yq2xkaIV45RudibfPUUs&gid=239462445"),
        [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]
    ),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true])
in
    PromotedHeaders;
shared SafeMapRoomType = let
  ExpectedColumns = {

    "roomtype_key",  
    "property_code",
    "roomtype_code", 
    "roomtype", 
    "roomtype_class", 
    "roomtype_category_code", 
    "roomtype_category_base", 
    "roomtype_category", 
    "roomtype_physical_capacity", 
    "roomtype_sort"

  },
  //
  ActualColumns = try Table.ColumnNames(map_roomtype_gsheet) otherwise {},
  SchemaValid = List.Intersect({ActualColumns, ExpectedColumns}) = ExpectedColumns,
  //
  SafeMapRoomTypeRaw = if SchemaValid 
      then map_roomtype_gsheet 
      else #table(ExpectedColumns, {}),
  //
  TransformColumns = Table.TransformColumnTypes(SafeMapRoomTypeRaw, {
  //
    {"roomtype_key", type text},
    {"property_code", type text},    
    {"roomtype_code", type text},
    {"roomtype", type text},
    {"roomtype_class", type text},
    {"roomtype_category_code", type text},
    {"roomtype_category_base", type text},
    {"roomtype_category", type text},
    {"roomtype_physical_capacity", Int64.Type},
    {"roomtype_sort", type text}

  }),
  //
  ReplaceErrors = Table.ReplaceErrorValues(TransformColumns, {
  //
    {"roomtype_key", null}, 
    {"property_code", null}, 
    {"roomtype_code", null},
    {"roomtype", null},
    {"roomtype_class", null},
    {"roomtype_category_code", null},
    {"roomtype_category_base", null},
    {"roomtype_category", null},
    {"roomtype_physical_capacity", null},
    {"roomtype_sort", null}
  //
  })
in
  ReplaceErrors;
shared SafeMapRoomTypeSchemaFallback = let
  EmptyMapRoomTypeTable = #table(
  //
    {
     //
    "roomtype_key",  
    "property_code",
    "roomtype_code", 
    "roomtype", 
    "roomtype_class", 
    "roomtype_category_code", 
    "roomtype_category_base", 
    "roomtype_category", 
    "roomtype_physical_capacity", 
    "roomtype_sort"
    //
      },
      {}
    ),
  TransformColumns = Table.TransformColumnTypes(EmptyMapRoomTypeTable, {
  //
    {"roomtype_key", type text},
    {"property_code", type text},    
    {"roomtype_code", type text},
    {"roomtype", type text},
    {"roomtype_class", type text},
    {"roomtype_category_code", type text},
    {"roomtype_category_base", type text},
    {"roomtype_category", type text},
    {"roomtype_physical_capacity", Int64.Type},
    {"roomtype_sort", type text}
    //
  }),
  //
  ReplaceErrors = Table.ReplaceErrorValues(TransformColumns, {
     //
    {"roomtype_key", null}, 
    {"property_code", null}, 
    {"roomtype_code", null},
    {"roomtype", null},
    {"roomtype_class", null},
    {"roomtype_category_code", null},
    {"roomtype_category_base", null},
    {"roomtype_category", null},
    {"roomtype_physical_capacity", null},
    {"roomtype_sort", null}
    //
  })
in
  ReplaceErrors;
shared SafeTypeChecker = // =====================================================
// 🔧 CONFIG: Define Schema Metadata
// =====================================================

let
    config = ValidationConfig,
    Source = config[SourceQuery],
    TypeList = config[TypeList],
    SourceName = config[SourceName],
    
// ================================================================
// 🔎 Get the Error Log
// ================================================================
    
    ErrorLogOutput = fnGenerateErrorLog(config[SourceQuery], config[TypeList], config[SourceName]),

// ================================================================
//  ✅ Get Cleaned Valid Data
// ================================================================

    CleanedData = fnCleanValidRows(config[SourceQuery], config[TypeList]),
    ErrorLog = CleanedData
in
    ErrorLog  // Or CleanedData depending on what you want to output
;
shared SafeTypeErrorLogBuilder = let
    config = ValidationConfig,
    Source = config[HelperQuery],
    TypeList = config[TypeList],
    SourceName = config[SourceName],
    //
    ErrorLog = fnGenerateErrorLog( config[HelperQuery], config[TypeList], config[SourceName] )
//  ErrorLog = fnGenerateErrorLog( *SourceQueryName*, *TypeList*, *SourceNameOrLabel ) This is the order and definitions for the passed values.
//
in
    ErrorLog;
shared fnGenerateErrorLog = // Function: fnGenerateErrorLog
// Purpose: Generates a concise and human-readable error log for type validation
// Output: Table with RowNumber, ColumnName, Value, ErrorType, ExpectedType, Source, Timestamp

(SourceTable as table, TypeList as list, SourceName as text) as table =>
let
    // 🕓 Capture current timestamp once for consistency
    Timestamp = DateTimeZone.FixedUtcNow(),

    // 🔢 Add RowNumber for traceability
    TableWithIndex = Table.AddIndexColumn(SourceTable, "RowNumber", 1, 1),

    // 🎯 Filter to columns that actually exist
    ValidTypes = List.Select(TypeList, each List.Contains(Table.ColumnNames(SourceTable), _{0})),

    // 🛠️ Create error entries for each column
    ErrorTables = List.Transform(ValidTypes, each 
        let 
            colName = _{0},
            colType = _{1},
            colOnly = Table.SelectColumns(TableWithIndex, {colName, "RowNumber"}),

            errors = Table.AddColumn(colOnly, "ErrorType", each 
                let v = Record.Field(_, colName)
                in if v = null then null 
                   else if not Value.Is(v, colType) then "InvalidType" 
                   else null, type text),

            flagged = Table.SelectRows(errors, each [ErrorType] <> null),

            withColName = Table.AddColumn(flagged, "ColumnName", each colName, type text),
            withType = Table.AddColumn(withColName, "ExpectedType", each Value.TypeName(colType), type text),
            withSource = Table.AddColumn(withType, "Source", each SourceName, type text),
            withTS = Table.AddColumn(withSource, "Timestamp", each Timestamp, type datetimezone),

            reordered = Table.SelectColumns(withTS, {"RowNumber", "ColumnName", colName, "ErrorType", "ExpectedType", "Source", "Timestamp"}),
            renamed = Table.RenameColumns(reordered, {{colName, "Value"}})
        in 
            renamed
    ),

    // 🧾 Combine all errors into final table
    ErrorLog = Table.Combine(ErrorTables)
in
    ErrorLog;
shared fnCleanValidRows = (SourceTable as table, TypeList as list) as table =>
let
    TableWithIndex = Table.AddIndexColumn(SourceTable, "RowNumber", 1, 1),
    ValidTypes = List.Select(TypeList, each List.Contains(Table.ColumnNames(SourceTable), _{0})),

    // Generate list of row numbers with errors
    ErrorRows = List.Combine(List.Transform(ValidTypes, each 
        let 
            colName = _{0},
            colType = _{1},
            colOnly = Table.SelectColumns(TableWithIndex, {colName, "RowNumber"}),
            errors = Table.SelectRows(colOnly, each 
                let v = Record.Field(_, colName)
                in v <> null and not Value.Is(v, colType))
        in errors[RowNumber]
    )),

    CleanedTable = Table.SelectRows(TableWithIndex, each not List.Contains(ErrorRows, [RowNumber])),
    RemoveIndex = Table.RemoveColumns(CleanedTable, {"RowNumber"})
in
    RemoveIndex;
shared SafeTypeFabricLogBuilder = let
    config = ValidationConfig,
    Source = config[HelperQuery],
    SourceName = config[SourceName],
    Timestamp = DateTimeZone.FixedUtcNow(),
    //
    TypeList = {
    //
    config[TypeList]
    },
    //
    // Step 1: Ensure Original RowIndex
    tblWithIndex = Table.AddIndexColumn(Source, "RowNumber", 1, 1),

    // Step 2: Filter to valid type targets
    validTypes = List.Select(TypeList, each List.Contains(Table.ColumnNames(Source), _{0})),

    // Step 3: Generate concise error records per column
    ErrorTables = List.Transform(validTypes, each 
        let 
            colName = _{0},
            colType = _{1},
            colOnly = Table.SelectColumns(tblWithIndex, {colName, "RowNumber"}),
            errors = Table.AddColumn(colOnly, "ErrorType", each 
                let v = Record.Field(_, colName)
                in if v = null then null 
                    else if not Value.Is(v, colType) then "InvalidType" 
                    else null, type text),
            flagged = Table.SelectRows(errors, each [ErrorType] <> null),
            withColName = Table.AddColumn(flagged, "ColumnName", each colName, type text),
            withType = Table.AddColumn(withColName, "ExpectedType", each Value.TypeName(colType), type text),
            withSource = Table.AddColumn(withType, config[HelperQuery], each config[SourceName], type text),
            withTS = Table.AddColumn(withSource, "Timestamp", each Timestamp, type datetimezone),
            reordered = Table.SelectColumns(withTS, {"RowNumber", "ColumnName", colName, "ErrorType", "ExpectedType", config[HelperQuery], "Timestamp"}),
            renamed = Table.RenameColumns(reordered, {{colName, "Value"}})
        in 
            renamed
    ),

    // Step 4: Final combined ErrorLog table
    ErrorLog = Table.Combine(ErrorTables)
in
    ErrorLog;
shared ErrorLog = let
    config = ValidationConfig,
    Source = config[HelperQuery],
    TypeList = config[TypeList],
    SourceName = config[SourceName],

    ErrorLog = fnGenerateErrorLog( config[HelperQuery], config[TypeList], config[SourceName] )
in
    ErrorLog;
shared ValidationConfig = let
    Config = [
            //
            SourceQuery = Pace_RoomType,
            HelperQuery = SafeTypeChecker,
            SourceName = "PaceData Segment Debug",
            //
            TypeList = {
            //
            //
            {"property_code", type nullable text, true, null},
            {"property_name", type nullable text, true, null},
            {"property_name_map", type nullable text, true, null},
            {"available_rms_map", type nullable number, true, each _ >= 0},
            {"hotel_shortname_map", type nullable text, true, null},
            {"segment", type nullable text, true, null},
            {"dow", type nullable text, text, true, null},
            {"stay_date", type nullable datetime, false, null},
            {"stay_date_py", type nullable datetime, false, null},
            {"rms_fct_py", type nullable number, true, each _ >= 0},
            {"rms_fct_cy", type nullable number, true, each _ >= 0},
            {"adr_fct_py", type nullable number, true, each _ >= 0},
            {"adr_fct_cy", type nullable number, true, each _ >= 0},
            {"rev_fct_cy", type nullable number, true, each _ >= 0},
            {"rev_fct_py", type nullable number, true, each _ >= 0},
            {"rms_cy", type nullable number, true, each _ >= 0},
            {"rms_py", type nullable number, true, each _ >= 0},
            {"rms_stly", type nullable number, true, each _ >= 0},
            {"rms_st2y", type nullable number, true, each _ >= 0},
            {"rms_st19", type nullable number, true, each _ >= 0},
            {"adr_py", type nullable number, true, each _ >= 0},
            {"adr_cy", type nullable number, true, each _ >= 0},
            {"rev_cy", type nullable number, true, each _ >= 0},
            {"rev_py", type nullable number, true, each _ >= 0},
            {"rev_stly", type nullable number, true, each _ >= 0},
            {"rev_st2y", type nullable number, true, each _ >= 0},
            {"rev_st19", type nullable number, true, each _ >= 0},
            {"arrivals_cy", type nullable number, true, each _ >= 0},
            {"arrivals_py", type nullable number, true, each _ >= 0},
            {"departures_cy", type nullable number, true, each _ >= 0},
            {"departures_py", type nullable number, true, each _ >= 0},
            {"no_show_py", type nullable number, true, each _ >= 0},
            {"no_show_cy", type nullable number, true, each _ >= 0},
            {"cx_py", type nullable number, true, each _ >= 0},
            {"cx_cy", type nullable number, true, each _ >= 0},
            {"property_fct_rms_cy", type nullable number, true, each _ >= 0},
            {"property_fct_rms_py", type nullable number, true, each _ >= 0},
            {"property_fct_rev_cy", type nullable number, true, each _ >= 0},
            {"property_fct_rev_py", type nullable number, true, each _ >= 0},
            {"bgt_rms_cy", type nullable number, true, each _ >= 0},
            {"bgt_rms_py", type nullable number, true, each _ >= 0},
            {"bgt_rev_cy", type nullable number, true, each _ >= 0},
            {"bgt_rev_py", type nullable number, true, each _ >= 0},
            {"segment_code", type nullable text, true, null},
            {"segment_sort", type nullable number, true, each _ >= 0},
            {"segment_group", type nullable text, true, null},
            {"segment_group_code", type nullable text, true, null},
            {"finance_segment", type nullable text, true, null},
            {"snapshot_date", type nullable datetime, false, null},
            {"ingested_timestamp", type nullable datetime, false, null},
            {"sent_to_big_query", type nullable logical, true, null},
            {"date_sent_to_big_query", type nullable datetime, false, null}
            //
            //Add more here...
            // 
            }
        ]
in
    Config;
shared fnExportToFabricLakehouse = // fnExportToFabricLakehouse
// 🔁 Append error log to a Fabric Lakehouse table

let
    fnExportToFabricLakehouse = (ErrorLogTable as table, DestinationTable as text) as table =>
    let
        Output = Table.Combine({ErrorLogTable}),
        ExportStep = 
            Table.RenameColumns(
                Output,
                List.Transform(Table.ColumnNames(Output), each {_, Text.Replace(_, " ", "_")})  // sanitize
            ),

        WriteStep = 
            Table.WriteToTable(ExportStep, DestinationTable, [Mode = "Append", EnableOverwrite = false])
    in
        WriteStep
in
    fnExportToFabricLakehouse;
shared GetColumnTypes = let
    // Combine tables for sampling
    GetTables = Table.Combine({Pace_RoomType}),
    SampleRow = Table.FirstN(GetTables, 1),
    ColumnSchema = Table.Schema(SampleRow),

    // Safe type mapping
    TypePairs = List.Transform(
        Table.ToRecords(ColumnSchema), 
        each 
            "    {""" & [Name] & """, " & 
            (
                if [Kind] = "Int64" or [Kind] = "Number" then "type nullable number"
                else if [Kind] = "Text" then "type nullable text"
                else if [Kind] = "DateTime" then "type nullable datetime"
                else if [Kind] = "Date" then "type nullable date"
                else if [Kind] = "Logical" then "type nullable logical"
                else if [Kind] = "Duration" then "type nullable duration"
                else "type any"
            ) & "}"
    ),

    CombinedTypeText = Text.Combine(TypePairs, "," & "#(cr)#(lf)"),

    OutputText = 
        "ChangedColumnType = Table.TransformColumnTypes(RenamedColumnHeaders, {" & 
        "#(cr)#(lf)" & 
        CombinedTypeText & "#(cr)#(lf)" & 
        "})"
in
    OutputText;
shared Pace_RoomType = let
    // Connect to SharePoint folder, exclude "_Archive", and filter for February-created Excel files
    Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/pace", [ApiVersion = 15]),
    FilteredFiles = Table.SelectRows(
        Source,
        each
            not Text.Contains([Folder Path], "_archive")
            and not Text.Contains([Folder Path], "_schema_templates")
            and not Text.Contains([Folder Path], "SEAPOP")
            and Text.EndsWith([Name], ".xlsx")
            and Text.Contains([Name], "202501")
    ),
    // 🚀 Extract and expand valid Excel sheets
    ExtractedWorkbooks = Table.AddColumn(
        FilteredFiles, "Workbook", each try Excel.Workbook([Content], null, true) otherwise null
    ),
    ValidWorkbooks = Table.SelectRows(ExtractedWorkbooks, each Type.Is(Value.Type([Workbook]), type table)),
    ExpandedSheets = Table.ExpandTableColumn(ValidWorkbooks, "Workbook", {"Name", "Data"}, {"SheetName", "Data"}),
    TargetSheets = Table.SelectRows(ExpandedSheets, each [SheetName] = "Room Type"),
    ValidTables = Table.SelectRows(TargetSheets, each Type.Is(Value.Type([Data]), type table)),
    // 🚀 Promote headers and clean column names
    PromotedHeaders = Table.TransformColumns(
        ValidTables, {"Data", each Table.PromoteHeaders(_, [PromoteAllScalars = true])}
    ),
    CleanedColumnNames = Table.TransformColumns(
        PromotedHeaders, {"Data", each Table.TransformColumnNames(_, each Text.Lower(Text.Replace(_, " ", "_")))}
    ),
    // 🚀 Expand data and add index
    ExpandedData =
        if Table.IsEmpty(CleanedColumnNames) then
            CleanedColumnNames
        else
            Table.ExpandTableColumn(CleanedColumnNames, "Data", Table.ColumnNames(CleanedColumnNames{0}[Data])),
    // 🚀 Extract "property_code" and "snapshot_date"
    ExtractedPropertyCode = Table.AddColumn(
        ExpandedData, "property_code", each if [Name] <> null then Text.Start([Name], 6) else null, type text
    ),
    ExtractedSnapshotDate = Table.AddColumn(
        ExtractedPropertyCode,
        "snapshot_date",
        each if [Name] <> null then Text.End(Text.Select(Text.From([Name]), {"0".."9"}), 8) else null,
        type text
    ),
    SnapshotDateToDateType = Table.TransformColumns(
        ExtractedSnapshotDate, {{"snapshot_date", each try Date.From(_) otherwise null, type nullable date}}
    ),
    // 🚀 Reorder and clean system-generated columns
    ReorderedColumns = Table.ReorderColumns(
        SnapshotDateToDateType,
        {"property_code", "property_name"}
            & List.RemoveItems(Table.ColumnNames(SnapshotDateToDateType), {"property_code", "property_name"})
    ),
    CleanedColumns = Table.RemoveColumns(
        ReorderedColumns,
        List.Select(
            Table.ColumnNames(ReorderedColumns),
            each Text.StartsWith(_, "column") and (try Number.From(Text.Middle(_, 6)) is number otherwise false)
        )
    ),
    // 🚀 Batch rename columns using dictionary rules
    ReplacementRules = [
        #"_-_" = "_",
        #"forecasted_room_revenue" = "rev_fct",
        #"last_room_value" = "lrv",
        #"_actual" = "",
        #"_on_books" = "",
        #"_total" = "",
        #"booked_" = "",
        #"_n/a" = "",
        #"_this_year" = "",
        #"_last_year" = "_py",
        #"day_of_week" = "dow",
        #"occupancy_date" = "stay_date",
        #"comparison_date" = "stay_date",
        #"room_revenue" = "rev",
        #"occupancy" = "rms",
        #"revenue" = "rev",
        #"rooms_sold" = "rms",
        #"rms_rev" = "rev",
        #"my_forecast" = "projected_fct",
        #"user_forecast" = "projected_fct",
        #"user_projected" = "projected_fct",
        #"user" = "projected",
        #"budget" = "bgt",
        #"forecast" = "fct",
        #"forecasted" = "fct",
        #"cancelled" = "cx",
        #"system" = "sys",
        #"out_of_order" = "ooo",
        #"%" = "pct",
        #"transient" = "trn",
        #"group" = "grp",
        #"property_constrained" = "constrained",
        #"property_unconstrained" = "unconstrained",
        #"property_demand" = "demand",
        #"rmss" = "rms",
        #"-" = "",
        #"room_type" = "rms_roomtype",
        #"room_class" = "rms_roomtype_class",
        #"rooms_ooo" = "rms_ooo",
        #"rooms_ooo_py" = "rms_ooo_py",
        #"rooms_other" = "rms_other",
        #"rooms_other_py" = "rms_other_py"
    ],
    ColumnNames = Table.ColumnNames(CleanedColumns),
    RenamedColumns = List.Transform(
        ColumnNames,
        each
            List.Accumulate(
                Record.FieldNames(ReplacementRules),
                _,
                (state, key) => Text.Replace(state, key, Record.Field(ReplacementRules, key))
            )
    ),
    FinalRenamedColumns = Table.RenameColumns(CleanedColumns, List.Zip({ColumnNames, RenamedColumns})),
    NewColumnNames = Table.ColumnNames(FinalRenamedColumns),
    // 🚀 Efficiently and safely change column types
    // ✅ Define all desired column/type pairs
    TypeChanges = {
        {"property_name", type text},
        {"physical_capacity", type number},
        {"property_shortname", type text},
        {"rms_roomtype", type text},
        {"rms_roomtype_class", type text},
        {"dow", type text},
        {"stay_date", type date},
        {"stay_date_py", type date},
        {"rms", Int64.Type},
        {"rms_py", Int64.Type},
        {"rms_stly", Int64.Type},
        {"rms_st2y", Int64.Type},
        {"rms_st19", Int64.Type},
        {"adr", type number},
        {"adr_py", type number},
        {"adr_fct", type number},
        {"adr_fct_py", type number},
        {"rms_ooo", Int64.Type},
        {"rms_ooo_py", Int64.Type},
        {"rms_other", type number},
        {"rms_other_py", type number},
        {"rev", type number},
        {"rev_py", type number},
        {"rev_stly", type number},
        {"rev_st2y", type number},
        {"rev_st19", type number},
        {"rev_fct", type number},
        {"rev_fct_py", type number},
        {"revpar", type number},
        {"revpar_py", type number},
        {"revpar_fct", type number},
        {"revpar_fct_py", type number},
        {"arrivals", Int64.Type},
        {"arrivals_py", Int64.Type},
        {"departures", Int64.Type},
        {"departures_py", Int64.Type},
        {"no_show", Int64.Type},
        {"no_show_py", Int64.Type},
        {"cx", Int64.Type},
        {"cx_py", Int64.Type},
        {"capacity", Int64.Type},
        {"capacity_py", Int64.Type},
        {"remaining_capacity", Int64.Type},
        {"remaining_capacity_py", Int64.Type},
        {"overbooking", Int64.Type},
        {"overbooking_py", Int64.Type},
        {"bar", type text},
        {"bar_py", type text}
    },
    // ✅ Filter the type-change list to only columns that exist
    ExistingColumns = Table.ColumnNames(FinalRenamedColumns),
    ExistingTypeChanges = List.Select(TypeChanges, each List.Contains(ExistingColumns, _{0})),
    // ✅ Apply the type conversions
    ConvertedTypes = Table.TransformColumnTypes(FinalRenamedColumns, ExistingTypeChanges),
    RemovedPropertyName = Table.RemoveColumns(ConvertedTypes, {"property_name"}, MissingField.Ignore),
    // 🚀 Add Lookup Called Room Type Key
    AddedRoomTypeKey = Table.TransformColumnTypes(
        Table.AddColumn(
            RemovedPropertyName, "roomtype_lookup", each let
                    property_code = Text.Lower(Text.Remove([property_code], {"(", ")", "[", "]"})),
                    rms_roomtype = if Text.Trim([rms_roomtype]) = "" or [rms_roomtype] = null then null
                        else Text.Lower(Text.Remove([rms_roomtype], {"(", ")", "[", "]"})) in if rms_roomtype = null then null
                    else  property_code & "_" & rms_roomtype),
        {{"roomtype_lookup", type text}}),
    //
    // MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
    // ✅ Add RoomType Data to Table from map_property Google Sheet
    //
    MapRoomTypeExists = try Table.RowCount(SafeMapRoomType) > 0 otherwise false,
    //
    MergedRoomTypeData = if MapRoomTypeExists then
            Table.NestedJoin(AddedRoomTypeKey, {"roomtype_lookup"}, SafeMapRoomType, {"roomtype_key"}, "map_roomtype", JoinKind.LeftOuter)
        else AddedRoomTypeKey,
    //
    SafeTypedRoomTypeData = Table.TransformColumns(
        MergedRoomTypeData,
        {
            {"roomtype_code", type text},
            {"roomtype", type text},
            {"roomtype_class", type text},
            {"roomtype_category_code", type text},
            {"roomtype_category_base", type text},
            {"roomtype_category", type text},
            {"roomtype_physical_capacity", Int64.Type},
            {"roomtype_sort", Int64.Type}
        }),
    //
    ExpandedRoomTypeData = if MapRoomTypeExists then
        Table.TransformColumns(
            Table.ExpandTableColumn(MergedRoomTypeData, "map_roomtype", {
                "roomtype_code", "roomtype", "roomtype_class",
                "roomtype_category_code", "roomtype_category_base", "roomtype_category",
                "roomtype_physical_capacity", "roomtype_sort"
            }),
            {
                {"roomtype_physical_capacity", each try Int64.From(_) otherwise 0},
                {"roomtype_sort", each try Int64.From(_) otherwise 0}
            }
        )
    else SafeTypedRoomTypeData,
    //
    TypedRoomTypeData = Table.TransformColumnTypes(ExpandedRoomTypeData, {{"roomtype_physical_capacity", Int64.Type}, {"roomtype_sort", Int64.Type}}),
    //
    FinalOutMergedRoomTypeData = TypedRoomTypeData,
    //
    // MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
    // ✅ Add Property Data to Table from map_property Google Sheet
    // 
    MapPropertyExists = try Table.RowCount(SafeMapProperty) > 0 otherwise false,
    //
    MergedPropertyData =if MapPropertyExists then
            Table.NestedJoin(FinalOutMergedRoomTypeData, {"property_code"}, SafeMapProperty, {"property_code_key"}, "map_property", JoinKind.LeftOuter)
        else FinalOutMergedRoomTypeData,
    //
    SafeTypedPropertyData = Table.TransformColumns(
        MergedPropertyData,
        {
            {"property_code", type text},
            {"property_shortname",  type text},
            {"physical_capacity", each try Int64.From(_) otherwise null, Int64.Type}
        }),
    //
    ExpandedPropertyData = if MapPropertyExists then
        Table.TransformColumnTypes(
            Table.ExpandTableColumn(MergedPropertyData, "map_property", {"property_name", "physical_capacity", "property_shortname"}),
            {{"physical_capacity", Int64.Type}})
        else SafeTypedPropertyData,
    //
    FinalOutputMergedPropertyData = ExpandedPropertyData,
    //
    // MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM

    AddedBQColumns = Table.AddColumn(
        Table.AddColumn(FinalOutputMergedPropertyData, "sent_to_big_query", each false, type logical),
        "date_sent_to_big_query",
        each null,
        type datetime
    ),
    WithIngestedTimestamp = Table.AddColumn(
        AddedBQColumns, "ingested_timestamp", each DateTime.LocalNow(), type datetime
    ),
    // 🚀 Safely define final column order and reorder output
    ColumnOrder = {
        "property_code",
        "property_name",
        "dow",
        "stay_date",
        "stay_date_py",
        "rms",
        "rms_py",
        "rms_stly",
        "rms_st2y",
        "rms_st19",
        "rms_other",
        "rms_other_py",
        "adr",
        "adr_py",
        "adr_fct_py",
        "adr_fct",
        "rev",
        "rev_py",
        "rev_stly",
        "rev_st2y",
        "rev_st19",
        "revpar",
        "revpar_py",
        "revpar_fct",
        "revpar_fct_py",
        "rev_fct",
        "rev_fct_py",
        "arrivals",
        "arrivals_py",
        "departures",
        "departures_py",
        "no_show",
        "no_show_py",
        "rms_ooo",
        "rms_ooo_py",
        "cx",
        "cx_py",
        "capacity",
        "capacity_py",
        "remaining_capacity",
        "remaining_capacity_py",
        "overbooking",
        "overbooking_py",
        "bar",
        "bar_py",
        "physical_capacity",
        "property_shortname",
        "rms_roomtype",
        "rms_roomtype_class",
        "roomtype_key",
        "roomtype_code",
        "roomtype_name",
        "roomtype_class",
        "roomtype_category_code",
        "roomtype_category_base",
        "roomtype_category",
        "roomtype_physical_capacity",
        "roomtype_sort",
        "snapshot_date",
        "ingested_timestamp",
        "sent_to_big_query",
        "date_sent_to_big_query"
    },
    // 🚀 Get only the columns that exist in the current dataset
    AvailableColumns = Table.ColumnNames(WithIngestedTimestamp),
    FinalColumnOrder = List.Intersect({ColumnOrder, AvailableColumns}),
    // 🚀 Reorder only the available ones, and let the rest stay as-is
    FinalColumnOutput = Table.ReorderColumns(WithIngestedTimestamp, FinalColumnOrder, MissingField.Ignore),
    // Filter dummy records
    RemoveDummyRows = Table.SelectRows(FinalColumnOutput, each not Text.Contains([property_name], "Dummy")),
    // Final output and preview options
    RemoveDummyRowsOutput = Table.RemoveColumns(
        RemoveDummyRows,
        {
            "Source.Name",
            "Content",
            "Attributes",
            "Name",
            "Extension",
            "Date accessed",
            "Date modified",
            "Date created",
            "Folder Path",
            "SheetName",
            "roomtype_lookup"
        },
        MissingField.Ignore
    ),
    // 🚀 Optional debugging
    ErroredRows = Table.SelectRows(
        RemoveDummyRowsOutput, each List.ContainsAny(Record.FieldValues(_), {null, "Error"})
    ),
    FinalOutput = RemoveDummyRowsOutput
in
    FinalOutput;
