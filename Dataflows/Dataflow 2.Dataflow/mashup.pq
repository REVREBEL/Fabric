[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"Transform Sample file" = let
    Source = Excel.Workbook(Parameter, null, true),
    Navigation = Source{[Item = "Room Type", Kind = "Sheet"]}[Data],
    PromotedHeaders = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),
    NormalizeHeader = (columnName as text) as text =>
        Text.Lower(Text.Replace(columnName, " ", "_")),
    RenamedHeaders = Table.TransformColumnNames(PromotedHeaders, NormalizeHeader),
    RemoveColumns = Table.RemoveColumns(RenamedHeaders,
        List.Select(Table.ColumnNames(RenamedHeaders), each Text.StartsWith(_, "column") and (try Number.From(Text.Middle(_, 6)) is number otherwise false))),
    RemovedPropertyNameColumn = Table.RemoveColumns(RemoveColumns, {"property_name"}, MissingField.Ignore),
    FinalOutput = RemovedPropertyNameColumn
in
    FinalOutput;
shared ColumnNames = let
    ColumnNames = Table.ColumnNames(#"Transform Sample file")
in
    ColumnNames;
shared #"Sample file" = let
  Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/pace", [ApiVersion = 15]),
  FilteredFiles = Table.SelectRows(Source, each 
        Text.Contains(Text.Lower([Folder Path]), "/_schema_templates/", Comparer.OrdinalIgnoreCase) and 
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
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
    Source = Excel.Workbook(Parameter, null, true),
    Navigation = Source{[Item = "Room Type", Kind = "Sheet"]}[Data],
    PromotedHeaders = Table.PromoteHeaders(Navigation, [PromoteAllScalars = true]),
    NormalizeHeader = (columnName as text) as text =>
        Text.Lower(Text.Replace(columnName, " ", "_")),
    RenamedHeaders = Table.TransformColumnNames(PromotedHeaders, NormalizeHeader),
    RemoveColumns = Table.RemoveColumns(RenamedHeaders,
        List.Select(Table.ColumnNames(RenamedHeaders), each Text.StartsWith(_, "column") and (try Number.From(Text.Middle(_, 6)) is number otherwise false))),
    RemovedPropertyNameColumn = Table.RemoveColumns(RemoveColumns, {"property_name"}, MissingField.Ignore),
    FinalOutput = RemovedPropertyNameColumn
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
    "crs_property_code",
    "property_city"
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
    {"crs_property_code", type text},
    {"property_city", type text}
  }),
  //
  ReplaceErrors = Table.ReplaceErrorValues(TransformColumns, {
    {"property_code_key", null},
    {"pms_property_code", null},
    {"property_name", null},
    {"physical_capacity", null},
    {"property_shortname", null},
    {"crs_property_code", null},
    {"property_city", null}
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
    "crs_property_code",
    "property_city"
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
    {"crs_property_code", type text},
    {"property_city", type text}
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
    {"crs_property_code", null},
    {"property_city", null}
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
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Pace_RoomType_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Pace_RoomType = let
    Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/pace", [ApiVersion = 15]),
    FilteredFiles = Table.SelectRows(Source, each 
        not Text.Contains([Folder Path], "_archive") and not Text.Contains([Folder Path], "_schema_templates") and not Text.Contains([Folder Path], "Exports") and
        Text.EndsWith([Name], ".xlsx") and
        Text.Contains([Name], "2025")
    ),
    //
    // ✅ Filter non-hidden files
    FilteredHiddenFiles = Table.SelectRows(FilteredFiles, each [Attributes]?[Hidden]? <> true),
    //
    // ✅ Apply the sample-based transform function
    InvokeCustomFunction = Table.AddColumn(FilteredHiddenFiles, "Transform file", each #"Transform file"([Content])),
    RenamedColumns = Table.RenameColumns(InvokeCustomFunction, {{"Name", "Source.Name"}}),
    RemovedOtherColumns = Table.SelectColumns(RenamedColumns, {"Source.Name", "Transform file"}),
    //
    // ✅ Remove errors (optional)
    RemovedErrors = Table.RemoveRowsWithErrors(RemovedOtherColumns, {"Transform file"}),
    //
    // ✅ Expand using normalized column names from sample
    ExpandedTableColumn = Table.ExpandTableColumn(
        RemovedErrors, "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
    //
    // 🚀 Extract "property_code" and "snapshot_date"
    ExtractedPropertyCode = Table.AddColumn(
        ExpandedTableColumn, "property_code", each if [Source.Name] <> null then Text.Start([Source.Name], 6) else null, type text
    ),
    ExtractedSnapshotDate = Table.AddColumn(
        ExtractedPropertyCode,
        "snapshot_date",
        each if [Source.Name] <> null then Text.End(Text.Select(Text.From([Source.Name]), {"0".."9"}), 8) else null,
        type text
    ),
    SnapshotDateToDateType = Table.TransformColumns(
        ExtractedSnapshotDate, {{"snapshot_date", each try Date.From(_) otherwise null, type nullable date}}
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
    OriginalColumnNames = Table.ColumnNames(SnapshotDateToDateType),
    RenamedColumnNames = List.Transform(
        OriginalColumnNames,
        each List.Accumulate(
            Record.FieldNames(ReplacementRules),
            _,
            (state, key) => Text.Replace(state, key, Record.Field(ReplacementRules, key))
        )
    ),
    RenamingPairs = List.Zip({OriginalColumnNames, RenamedColumnNames}),
    FinalRenamedColumns = Table.RenameColumns(SnapshotDateToDateType, RenamingPairs),
    // 🚀 Efficiently and safely change column types
    // ✅ Define all desired column/type pairs
    TypeChanges = {
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
    FilteredRoomTypeData = Table.SelectRows(
        RemovedPropertyName,
        each not (Record.FieldOrDefault(_, "rms_roomtype", null) = null or Record.Field(_, "rms_roomtype") = "")
    ),
    AddedRoomTypeKey = Table.TransformColumnTypes(
        Table.AddColumn(
            FilteredRoomTypeData, "roomtype_lookup", each let
                    property_code = Text.Lower(Text.Remove([property_code], {"(", ")", "[", "]"})),
                    rms_roomtype = if Text.Trim([rms_roomtype]) = "" or [rms_roomtype] = null then null
                        else Text.Lower(Text.Remove([rms_roomtype], {"(", ")", "[", "]"})) in if rms_roomtype = null then null
                    else  property_code & "_" & rms_roomtype),
        {{"roomtype_lookup", type text}}),
    //
    // MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
    // ✅ Add RoomType Data to Table from map_property Google Sheet
    //
    // 🚀 Trim and Merge Segment Code Lookup Table
    CleanText = (text) => Text.Trim(Text.Clean(text)),
    TrimmedRoomTypeData = Table.TransformColumns(AddedRoomTypeKey, {{"roomtype_lookup", CleanText, type text}}),
    TrimmedMapRoomType = Table.TransformColumns(SafeMapRoomType, {{"roomtype_key", CleanText, type text}}),
    //
    //
    MapRoomTypeExists = try Table.RowCount(TrimmedMapRoomType) > 0 otherwise false,
    //
    MergedRoomTypeData = if MapRoomTypeExists then
            Table.NestedJoin(TrimmedRoomTypeData, {"roomtype_lookup"}, TrimmedMapRoomType, {"roomtype_key"}, "map_roomtype", JoinKind.LeftOuter)
        else TrimmedRoomTypeData,
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
    FinalOutputMergedRoomTypeData = TypedRoomTypeData,
    //
    // MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
    // ✅ Add Property Data to Table from map_property Google Sheet
    // 
    MapPropertyExists = try Table.RowCount(SafeMapProperty) > 0 otherwise false,
    //
    MergedPropertyData =if MapPropertyExists then
            Table.NestedJoin(FinalOutputMergedRoomTypeData, {"property_code"}, SafeMapProperty, {"property_code_key"}, "map_property", JoinKind.LeftOuter)
        else FinalOutputMergedRoomTypeData,
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
            Table.ExpandTableColumn(MergedPropertyData, "map_property", {"property_name", "property_shortname", "physical_capacity"}),
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
        "property_code", "property_name", "dow", "stay_date", "stay_date_py", 
        "rms_roomtype", "roomtype", 
        "rms", "rms_py", "rms_stly", "rms_st2y", "rms_st19", "rms_other", "rms_other_py", 
        "adr", "adr_py", "adr_fct_py", "adr_fct", "rev", "rev_py", "rev_stly", "rev_st2y", "rev_st19", 
        "revpar", "revpar_py", "revpar_fct", "revpar_fct_py", "rev_fct", "rev_fct_py", 
        "arrivals", "arrivals_py", "departures", "departures_py", "no_show", "no_show_py", 
        "rms_ooo", "rms_ooo_py", "cx", "cx_py", 
        "capacity", "capacity_py", "remaining_capacity", "remaining_capacity_py", "overbooking", "overbooking_py", 
        "bar", "bar_py", "physical_capacity", 
        "property_shortname", 
        "rms_roomtype", "rms_roomtype_class", "roomtype_code", "roomtype_class", 
        "roomtype_category_code", "roomtype_category_base", "roomtype_category", "roomtype_physical_capacity", "roomtype_sort", 
        "snapshot_date", "ingested_timestamp", 
        "sent_to_big_query", "date_sent_to_big_query"
    },
    // 🚀 Get only the columns that exist in the current dataset
    AvailableColumns = Table.ColumnNames(WithIngestedTimestamp),
    FinalColumnOrder = List.Intersect({ColumnOrder, AvailableColumns}),
    // 🚀 Reorder only the available ones, and let the rest stay as-is
    FinalColumnOutput = Table.ReorderColumns(WithIngestedTimestamp, FinalColumnOrder, MissingField.Ignore),
    // ✅ Filter dummy records
    RemoveDummyRows = Table.SelectRows(FinalColumnOutput, each not Text.Contains([property_name], "Dummy")),
    // ✅ Final output and preview options
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
            "roomtype_key",
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
shared Pace_RoomType_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "6fc7a30e-b793-4185-b9d5-2cbe481efae7"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "f266312a-87c6-47eb-aff9-fd34597688fb"]}[Data],
  TableNavigation = Navigation_2{[Id = "Pace_RoomType", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
