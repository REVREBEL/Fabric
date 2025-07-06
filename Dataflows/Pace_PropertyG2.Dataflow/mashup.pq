[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Pace_Property_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Pace_Property = let
    Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/pace", [ApiVersion = 15]),
    FilteredFiles = Table.SelectRows(Source, each 
        not Text.Contains([Folder Path], "_archive") and not Text.Contains([Folder Path], "_schema_templates") and not Text.Contains([Folder Path], "Exports") and
        Text.EndsWith([Name], ".xlsx") and
        Text.Contains([Name], "2025")
    ),
    //
    //  Filter non-hidden files
    FilteredHiddenFiles = Table.SelectRows(FilteredFiles, each [Attributes]?[Hidden]? <> true),
    //
    // Apply the sample-based transform function
    InvokeCustomFunction = Table.AddColumn(FilteredHiddenFiles, "Transform file", each #"Transform file"([Content])),
    RenamedColumns = Table.RenameColumns(InvokeCustomFunction, {{"Name", "Source.Name"}}),
    RemovedOtherColumns = Table.SelectColumns(RenamedColumns, {"Source.Name", "Transform file"}),
    //
    // Step 2: Remove errors (optional)
    RemovedErrors = Table.RemoveRowsWithErrors(RemovedOtherColumns, {"Transform file"}),
    //
    // Expand using normalized column names from sample
    ExpandedTableColumn = Table.ExpandTableColumn(
        RemovedErrors, "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
    //
    // Extract "property_code" and "snapshot_date"
    ExtractedPropertyCode = Table.AddColumn(ExpandedTableColumn, "property_code", each if [Source.Name] <> null then Text.Start([Source.Name], 6) else null, type text),
    //
    ExtractedSnapshotDate = Table.AddColumn(ExtractedPropertyCode, "snapshot_date", each 
    if [Source.Name] <> null then Text.End(Text.Select(Text.From([Source.Name]), {"0".."9"}), 8) else null, type text),
    //
    SnapshotDateToDateType = Table.TransformColumns(ExtractedSnapshotDate, {{"snapshot_date", each try Date.From(_) otherwise null, type nullable date}}),
    //
    CorrectedDates = Table.TransformColumns(SnapshotDateToDateType, {{"occupancy_date", each Date.From(_), type date}, {"comparison_date_last_year", each Date.From(_), type date}}),
    //
    // Remove columnXX artifacts
    RemoveColumns = Table.RemoveColumns(CorrectedDates, List.Select(Table.ColumnNames(SnapshotDateToDateType), each Text.StartsWith(_, "column") and (try Number.From(Text.Middle(_, 6)) is number otherwise false))),
    //
    // Apply Replacement Rules
    ReplacementRules = [
        #"_-_" = "_", #"forecasted_room_revenue" = "rev_fct", #"last_room_value" = "lrv",
        #"_actual" = "", #"_on_books" = "", #"_total" = "", #"booked_" = "", #"_n/a" = "",
        #"_this_year" = "", #"_last_year" = "_py", #"day_of_week" = "dow", #"occupancy_date" = "stay_date", #"comparison_date" = "stay_date",
        #"room_revenue" = "rev", #"occupancy" = "rms", #"revenue" = "rev", #"rooms_sold" = "rms", #"rms_rev" = "rev",
        #"my_forecast" = "projected", #"user_forecast" = "projected", #"property_forecast" = "projected", #"user_projected" = "projected", #"user" = "projected", 
        #"budget" = "bgt", #"forecast" = "fct", #"forecasted" = "fct",
        #"cancelled" = "cx", #"system" = "sys", #"out_of_order" = "ooo", #"%" = "pct",           
        #"transient" = "trn", #"group" = "grp",  
        #"projected_constrained" = "constrained",  #"projected_unconstrained" = "unconstrained",  #"projected_demand" = "demand", 
        #"user_constrained" = "constrained",  #"user_unconstrained" = "unconstrained",  #"user_demand" = "demand", 
        #"rmss" = "rms",  #"-" = "", #"room_type" = "rms_roomtype", #"room_class" = "rms_roomtype_class",  #"rooms_ooo" = "rms_ooo",  #"rooms_ooo_py" = "rms_ooo_py",
        #"rooms_other" = "rms_other", #"rooms_other_py" = "rms_other_py", #"business_view" = "segment"
    ],
    //
    OriginalColumnNames = Table.ColumnNames(RemoveColumns),
    RenamedColumnNames = List.Transform(
        OriginalColumnNames,
        each List.Accumulate(
            Record.FieldNames(ReplacementRules),
            _,
            (state, key) => Text.Replace(state, key, Record.Field(ReplacementRules, key))
        )
    ),
    RenamingPairs = List.Zip({OriginalColumnNames, RenamedColumnNames}),
    FinalRenamedColumns = Table.RenameColumns(RemoveColumns, RenamingPairs),
    //
    // Step 8: Efficiently change column types
    TypeChanges = {
    {"property_code", type text},
    {"dow", type text}, {"stay_date", type date}, {"stay_date_py", type date},
    {"rms_fct", Int64.Type}, {"rms_fct_trn", Int64.Type}, {"rms_fct_grp", Int64.Type}, 
    {"rms_fct_py", Int64.Type}, {"rms_fct_trn_py", Int64.Type}, {"rms_fct_grp_py", Int64.Type},
    {"rev_fct", type number}, {"rev_fct_py", type number},
    {"rms", Int64.Type}, {"rms_py", Int64.Type}, {"rms_stly", Int64.Type}, {"rms_st2y", Int64.Type}, {"rms_st19", Int64.Type},
    {"adr", type number}, {"adr_py", type number}, {"adr_fct", type number}, {"adr_fct_py", type number}, 
    {"rev", type number}, {"rev_py", type number}, {"rev_stly", type number}, {"rev_st2y", type number}, {"rev_st19", type number}, 
    {"revpar", type number}, {"revpar_py", type number}, {"revpar_fct", type number}, {"revpar_fct_py", type number},
    {"rms_trn", Int64.Type}, {"rms_trn_py", Int64.Type}, {"rms_trn_stly", Int64.Type}, {"rms_trn_st2y", Int64.Type}, {"rms_trn_st19", Int64.Type},
    {"rms_grp", Int64.Type}, {"rms_grp_py", Int64.Type}, {"rms_grp_stly", Int64.Type}, {"rms_grp_st2y", Int64.Type}, {"rms_grp_st19", Int64.Type},
    {"arrivals", Int64.Type}, {"arrivals_py", Int64.Type}, {"departures", Int64.Type}, {"departures_py", Int64.Type},
    {"rms_ooo", Int64.Type}, {"rms_ooo_py", Int64.Type}, {"rms_other", Int64.Type}, {"rms_other_py", Int64.Type},
    {"cx", Int64.Type}, {"cx_py", Int64.Type}, {"no_show", Int64.Type}, {"no_show_py", Int64.Type},
    {"bar", type text}, {"lrv_py", type number}, {"lrv", type number},
    {"overbooking_py", Int64.Type}, {"overbooking", Int64.Type}, {"wash_pct", type number}, {"wash_pct_py", type number},
    {"special_event", type text}, {"special_event_py", type text},
    {"demand", Int64.Type}, {"demand_py", Int64.Type},
    {"unconstrained_demand_trn_py", Int64.Type}, {"unconstrained_demand_trn", Int64.Type}, 
    {"constrained_demand_grp_py", Int64.Type}, {"constrained_demand_grp", Int64.Type}, 
    {"projected_rms", Int64.Type}, {"projected_rms_py", Int64.Type}, {"projected_rev", type number}, {"projected_rev_py", type number}, 
    {"bgt_rms", Int64.Type},{"bgt_rms_py", Int64.Type}, {"bgt_rev", type number}, {"bgt_rev_py", type number},
    {"remaining_capacity_py", Int64.Type}, {"remaining_capacity", Int64.Type}, {"physical_capacity_py", Int64.Type}, {"physical_capacity", Int64.Type},
    {"sys_demand_py", Int64.Type}, {"sys_demand", type number}, {"sys_demand_grp", type number}, {"sys_demand_trn", type number},
    {"sys_demand_trn_py", Int64.Type}, {"sys_demand_grp_py", Int64.Type}

    },
    // ✅ Filter the type-change list to only columns that exist
    ExistingColumns = Table.ColumnNames(FinalRenamedColumns),
    ExistingTypeChanges = List.Select(TypeChanges, each List.Contains(ExistingColumns, _{0})),
    //
    // ✅ Apply the type conversions
    ConvertedTypes = Table.TransformColumnTypes(FinalRenamedColumns, ExistingTypeChanges),
    //
    // MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
    // ✅ Add Property Data to Table from map_property Google Sheet
    // 
    MapPropertyExists = try Table.RowCount(SafeMapProperty) > 0 otherwise false,
    //
    MergedPropertyData =
        if MapPropertyExists then
            Table.NestedJoin(
                ConvertedTypes, {"property_code"}, SafeMapProperty, {"property_code_key"}, "map_property", JoinKind.LeftOuter)
        else ConvertedTypes,
    //
    SafeTypedPropertyData = Table.TransformColumns(
        MergedPropertyData,
        {
            {"property_code", type text},
            {"property_shortname",  type text}
        }),
    //
    ExpandedPropertyData =
        if MapPropertyExists then
            Table.TransformColumnTypes(
                Table.ExpandTableColumn(
                    MergedPropertyData, "map_property", {"property_name", "property_shortname"}
                ),
                {
                    {"property_name", type text},
                    {"property_shortname", type text}
                }
            )
        else SafeTypedPropertyData,
    FinalOutputMergedPropertyData = ExpandedPropertyData,
    //
    // MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
    //
    AddedBQColumns = Table.AddColumn(Table.AddColumn(FinalOutputMergedPropertyData, "sent_to_big_query", each false, type logical), "date_sent_to_big_query", each null, type datetime),
    //
    WithIngestedTimestamp = Table.AddColumn(AddedBQColumns, "ingested_timestamp", each DateTime.LocalNow(), type datetime),
    //
    // Define the desired column order
    ColumnOrder = {
        "property_code",
        "dow", "stay_date", "stay_date_py",
        "rms_fct", "rms_fct_py", 
        "rms_fct_trn", "rms_fct_trn_py",
        "rms_fct_grp", "rms_fct_grp_py",
        "adr_fct", "adr_fct_py",
        "rev_fct", "rev_fct_py",
        "revpar_fct", "revpar_fct_py",
        "rms", "rms_py", "rms_stly", "rms_st2y",  "rms_st19",
        "adr","adr_py",
        "rev","rev_py","rev_stly","rev_st2y","rev_st19",
        "revpar", "revpar_py",
        "rms_sold_grp", "rms_sold_grp_py", "rms_sold_grp_stly", "rms_sold_grp_st2y", "rms_sold_grp_st19",
        "rms_sold_trn", "rms_sold_trn_py", "rms_sold_trn_stly", "rms_sold_trn_st2y", "rms_sold_trn_st19",
        "arrivals", "arrivals_py", "departures", "departures_py",
        "rms_ooo", "rms_ooo_py", "rms_other", "rms_other_py",
        "cx", "cx_py", "no_show", "no_show_py",
        "bar", "lrv_py", "lrv",
        "overbooking_py", "overbooking", "wash_pct", "wash_pct_py",
        "special_event", "special_event_py",
        "demand", "demand_py",
        "unconstrained_demand_trn_py", "unconstrained_demand_trn", 
        "constrained_demand_grp_py", "constrained_demand_grp", 
        "projected_rms", "projected_rms_py", "projected_rev", "projected_rev_py",
        "bgt_rms", "bgt_rms_py", "bgt_rev", "bgt_rev_py",
        "remaining_capacity_py", "remaining_capacity", "physical_capacity_py", "physical_capacity",
        "sys_demand_py", "sys_demand", "sys_demand_grp", "sys_demand_trn",
        "sys_demand_trn_py", "sys_demand_grp_py",
        "property_name", "property_shortname",
        "snapshot_date", "ingested_timestamp", "sent_to_big_query", "date_sent_to_big_query"
    },
    // 🚀 Get only the columns that exist in the current dataset
    AvailableColumns = Table.ColumnNames(WithIngestedTimestamp),
    FinalColumnOrder = List.Intersect({ColumnOrder, AvailableColumns}),
    //
    // 🚀 Reorder only the available ones, and let the rest stay as-is
    FinalColumnOrderOutput = Table.ReorderColumns(WithIngestedTimestamp, FinalColumnOrder, MissingField.Ignore),
    //
    // Filter dummy records
    RemoveDummyRows = Table.SelectRows(FinalColumnOrderOutput, each not Text.Contains([property_name], "Dummy")),
    //
    // Final output and preview options
    RemoveDummyRowsOutput = Table.RemoveColumns(RemoveDummyRows, {
        "Source.Name", "Content", "Attributes", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Folder Path", "SheetName"
        }, MissingField.Ignore),
    //
    FinalOutput = RemoveDummyRowsOutput
in
    FinalOutput;
shared #"Transform Sample file" = let
    Source = Excel.Workbook(Parameter, null, true),
    Navigation = Source{[Item = "Property", Kind = "Sheet"]}[Data],
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
shared #"Sample file" = let
  // -- ✅ Load files from SharePoint & Filter Relevant Files
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
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
    Source = Excel.Workbook(Parameter, null, true),
    Navigation = Source{[Item = "Property", Kind = "Sheet"]}[Data],
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
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
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
shared map_segment_gsheet = let
    Source = Csv.Document(
        Web.Contents("https://docs.google.com/spreadsheets/d/1JERzvRDvVbwfdEhFwF6JtwLLJ2md-IFFd5KLnS1ZRo8/export?format=csv&id=1JERzvRDvVbwfdEhFwF6JtwLLJ2md-IFFd5KLnS1ZRo8&gid=1439227510"),
        [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]
    ),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true])
in
    #"Promoted Headers";
shared SafeMapSegment = let
      ExpectedColumns = {
      //
      "segment", 
      "segment_code",
      "segment_sort",
      "segment_group",
      "segment_group_code", 
      "finance_segment"
      //
      },
      //
      ActualColumns = try Table.ColumnNames(map_segment_gsheet) otherwise {},
      SchemaValid = List.Difference(ExpectedColumns, ActualColumns) = {},

      SafeMapSegmentRaw = if SchemaValid 
      then map_segment_gsheet 
      else #table(ExpectedColumns, {}),
      TransformColumns = Table.TransformColumnTypes(SafeMapSegmentRaw, {
      //
      {"segment", type text},
      {"segment_code", type text},
      {"segment_sort", type number},
      {"segment_group", type text},
      {"segment_group_code", type text},
      {"finance_segment", type text}
    //
      }),
      //
      ReplaceErrors = Table.ReplaceErrorValues(TransformColumns, {
      //
      {"segment", null},
      {"segment_code", null},
      {"segment_sort", null},
      {"segment_group", null},
      {"segment_group_code", null},
      {"finance_segment", null}
      //
      })
in
      ReplaceErrors;
shared SafeMapSegmentSchemaFallback = let
  EmptyMapSegmentTable = #table(
  //
    {
     //
    "segment", 
    "segment_code",
    "segment_sort",
    "segment_group",
    "segment_group_code", 
    "finance_segment"
    //
      },
      {}
    ),
  TransformColumns = Table.TransformColumnTypes(EmptyMapSegmentTable, {
  //
    {"segment", type text},
    {"segment_code", type text},
    {"segment_sort", type number},
    {"segment_group", type text},
    {"segment_group_code", type text},
    {"finance_segment", type text}
    //
  }),
  //
  ReplaceErrors = Table.ReplaceErrorValues(TransformColumns, {
     //
    {"segment", null},
    {"segment_code", null},
    {"segment_sort", null},
    {"segment_group", null},
    {"segment_group_code", null},
    {"finance_segment", null}
    //
  })
in
  ReplaceErrors;
shared Pace_Property_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "6fc7a30e-b793-4185-b9d5-2cbe481efae7"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "f266312a-87c6-47eb-aff9-fd34597688fb"]}[Data],
  TableNavigation = Navigation_2{[Id = "Pace_Property", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
