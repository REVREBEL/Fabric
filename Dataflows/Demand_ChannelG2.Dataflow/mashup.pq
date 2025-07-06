[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
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
    
    RemovedColumns = Table.RemoveColumns(RenamedHeaders, {
      "occ_total", "cs_occ_total", "occ_index_total", "occ_rank_total", "occ_index_var_py_total", "occ_index_chg_pw_total", 
      "rms_sold_total", "rms_sold_pct_var_py_total", "cs_rms_sold_pct_var_py_total", "rms_sold_chg_pw_total", 
      "rms_sold_pct_chg_pw_total", "cs_rms_sold_pct_chg_pw_total", 
      "adr_total", "adr_rank_total", "revpar_total", "revpar_rank_total"
    }),
    
    TypedColumns = Table.TransformColumnTypes(RemovedColumns, {
      {"channel", type text},
      {"month", type text},
      {"stay_date", type datetime}
    })
in
    TypedColumns;
shared #"Sample file" = let
  // -- ✅ Step 1: Load files from SharePoint & Filter Relevant Files
  Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/demand", [ApiVersion = 15]),
  FilteredFiles = Table.SelectRows(Source, each 
        Text.Contains(Text.Lower([Folder Path]), "/_schema_templates/", Comparer.OrdinalIgnoreCase)  and 
        [Extension] = ".csv" and 
        ([Attributes]?[Hidden]? <> true)
    ),
  TemplateFile = Table.SelectRows(FilteredFiles, each Text.Contains([Name], "D360_T1_Channel_Mix_Schema.csv")),
  FirstFile = try TemplateFile{0}[Content] otherwise error "D360 Channel Mix Schema template file not found"
in
  FirstFile;
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",", Columns = 35, Encoding = 65001]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"month", type text}, {"channel", type text}, {"stay_date", type date}, {"occ_total", Percentage.Type}, {"cs_occ_total", Percentage.Type}, {"occ_index_total", Int64.Type}, {"occ_rank_total", type text}, {"occ_index_var_py_total", Int64.Type}, {"occ_index_chg_pw_total", Int64.Type}, {"rms_sold_total", Int64.Type}, {"rms_sold_pct_var_py_total", Percentage.Type}, {"cs_rms_sold_pct_var_py_total", Percentage.Type}, {"rms_sold_chg_pw_total", Int64.Type}, {"rms_sold_pct_chg_pw_total", Percentage.Type}, {"cs_rms_sold_pct_chg_pw_total", Percentage.Type}, {"adr_total", Currency.Type}, {"adr_rank_total", type text}, {"revpar_total", Currency.Type}, {"revpar_rank_total", type text}, {"occ", Percentage.Type}, {"cs_occ", Percentage.Type}, {"occ_index", Int64.Type}, {"occ_rank", type text}, {"occ_index_var_py", Int64.Type}, {"occ_index_chg_pw", Int64.Type}, {"rms_sold", Int64.Type}, {"rms_sold_pct_var_py", Percentage.Type}, {"cs_rms_sold_pct_var_py", Percentage.Type}, {"rms_sold_chg_pw", Int64.Type}, {"rms_sold_pct_chg_pw", Percentage.Type}, {"cs_rms_sold_pct_chg_pw", Percentage.Type}, {"adr", Currency.Type}, {"adr_rank", type text}, {"revpar", Currency.Type}, {"revpar_rank", type text}})
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Demand_Channel_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Demand_Channel = let
    //-- ✅ Get column names from sample file
    ColumnNames = Table.ColumnNames(#"Transform file"(#"Sample file")),

    //-- ✅ Load files from SharePoint
    Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/demand", [ApiVersion = 15]),

    //-- ✅ Filter for PMS folder and Channel_Mix CSVs
    FilteredFiles = Table.SelectRows(Source, each 
        Text.Contains(Text.Lower([Folder Path]), "/_database/") and not
        Text.Contains(Text.Lower([Folder Path]), "/_schema_templates/") and     
        Text.Contains([Name], "Channel_Mix") and
        //Text.Contains([Name], "DTWDFH") and
        [Extension] = ".csv"
    ),

    //-- ✅ Filter out hidden files
    FilteredHiddenFiles = Table.SelectRows(FilteredFiles, each [Attributes]?[Hidden]? <> true),
    InvokeTransform = Table.AddColumn(FilteredHiddenFiles, "Transformed Data", each #"Transform file"([Content])),
    RenamedColumns = Table.RenameColumns(InvokeTransform, {{"Name", "Source.Name"}}),
    RemovedOtherColumns = Table.SelectColumns(RenamedColumns, {"Source.Name", "Transformed Data"}),
    ExpandedTable = Table.ExpandTableColumn(RemovedOtherColumns, "Transformed Data", ColumnNames),
    //
    TypedColumns = Table.TransformColumnTypes(ExpandedTable, {
    //
            {"stay_date", type date}, {"channel", type text}, {"month", type text}, {"occ", type number}, {"cs_occ", type number}, {"occ_index", type number}, 
            {"occ_rank", type text}, {"occ_index_var_py", type number}, {"occ_index_chg_pw", type number}, 
            {"rms_sold", Int64.Type}, {"rms_sold_pct_var_py", type number}, {"cs_rms_sold_pct_var_py", type number}, 
            {"rms_sold_chg_pw", type number}, {"rms_sold_pct_chg_pw", type number}, {"cs_rms_sold_pct_chg_pw", type number}, 
            {"adr", type number}, {"adr_rank", type text}, {"revpar", type number}, {"revpar_rank", type text}
    //
    }),
    //
    RemovedColumns = Table.RemoveColumns(TypedColumns, {
    //
            "occ_total", "cs_occ_total", "occ_index_total", "occ_rank_total", "occ_index_var_py_total", "occ_index_chg_pw_total", 
            "rms_sold_total", "rms_sold_pct_var_py_total", "cs_rms_sold_pct_var_py_total", "rms_sold_chg_pw_total", 
            "rms_sold_pct_chg_pw_total", "cs_rms_sold_pct_chg_pw_total", 
            "adr_total", "adr_rank_total", "revpar_total", "revpar_rank_total"
    //
    }),
    //
    RenamedColumnHeaders = Table.RenameColumns(RemovedColumns, {
    //
            {"month","month_year"} ,{"channel","d360_channel"},
            {"rms_sold","rms"} ,{"rms_sold_pct_var_py","rms_pct_var_py"} , 
            {"cs_rms_sold_pct_var_py" , "cs_rms_pct_var_py"}, {"rms_sold_chg_pw","rms_chg_pw"} ,
            {"rms_sold_pct_chg_pw","rms_pct_chg_pw"} ,{"cs_rms_sold_pct_chg_pw","cs_rms_pct_chg_pw"} 
    //
    }),
     //-- ✅ Extract snapshot_date
    CreateSnapshotDate = Table.AddColumn(RenamedColumnHeaders, "snapshot_date", each try Text.Start([Source.Name], 8) otherwise null, type text),
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
    //
    //-- ✅ Add cs_demand_id from the File Name 
    AddCsDemandId = Table.AddColumn(SafeSetIdTyped,"cs_demand_id", each try Text.BetweenDelimiters([Source.Name], "IngestFile_", "_Set") otherwise null, type text),
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
    // ✅ Add Channel Data to Table from map_property Google Sheet
    //
    // ✂️ Step 1: Clean channel keys for matching
    CleanText = (text) => Text.Trim(Text.Clean(text)),
    TrimmedChannelData = Table.TransformColumns(FinalOutputMergedPropertyData, {{"d360_channel", CleanText, type text}}),
    TrimmedMapChannel = Table.TransformColumns(SafeMapDemandChannel, {{"channel_key", CleanText, type text}}),

    MapChannelDataExists = try Table.RowCount(TrimmedMapChannel) > 0 otherwise false,

    MergedChannelData = if MapChannelDataExists then 
        Table.NestedJoin(
            TrimmedChannelData, {"d360_channel"},
            TrimmedMapChannel, {"channel_key"},
            "map_demand_channel", JoinKind.LeftOuter)
        else FinalOutputMergedPropertyData,

    WithTypedChannelColumns = if MapChannelDataExists then 
        Table.TransformColumnTypes(
            Table.ExpandTableColumn(
                MergedChannelData, "map_demand_channel", 
                {"channel_group_code", "channel_group", "source_group_code", "source_group", "source_code", "source", "channel_sort"}
            ),
            {{"channel_sort", Int64.Type}}
        )
        else MergedChannelData,

    FinalOutputMergedChannelData = WithTypedChannelColumns,
    //
    // MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM    
    // ✅ Add Property Data to Table from map_property Google Sheet
    WithIngestedTimestamp = Table.AddColumn(FinalOutputMergedChannelData, "ingested_timestamp", each DateTime.LocalNow(), type datetime),
    ColumnOrder = {
        "property_code", "property_name", 
        "stay_date", "d360_channel",  "channel_group",  "source",
        "occ", "cs_occ", "occ_index", "occ_rank", 
        "occ_index_var_py", "occ_index_chg_pw", 
        "rms", "rms_pct_var_py", "cs_rms_pct_var_py", 
        "rms_chg_pw", "rms_pct_chg_pw", "cs_rms_pct_chg_pw", 
        "adr", "adr_rank", "revpar", "revpar_rank", 
        "cy", "month_year", "month", "weekday", "dow", 
        "channel_group_code",  "source_group_code", "source_group",  "source_code", "channel_sort",
        "property_shortname",  "cs_set_id", "physical_capacity", "cs_physical_capacity", 
        "cs_demand_id", "snapshot_date", "ingested_timestamp", "sent_to_big_query", "date_sent_to_big_query"
    },
    AvailableColumns = Table.ColumnNames(WithIngestedTimestamp),
    ReorderedColumnsList = List.Intersect({ColumnOrder, AvailableColumns}),
    ReorderedTable = Table.ReorderColumns(WithIngestedTimestamp, ReorderedColumnsList, MissingField.Ignore),
    FinalOutput = Table.RemoveColumns(ReorderedTable, {"Source.Name", "demand_cs_lookup", "demand_cs_key"}, MissingField.Ignore)
in
    FinalOutput;
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
        Web.Contents("https://docs.google.com/spreadsheets/d/19r9FdllD9Mn3zZ4dr8Ss4S5Yq2xkaIV45RudibfPUUs/export?format=csv&id=19r9FdllD9Mn3zZ4dr8Ss4S5Yq2xkaIV45RudibfPUUs&gid=2119579921"),
        [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]
    ),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true])
in
    #"Promoted Headers";
shared SafeMapCompset = let
      ExpectedColumns = {
            "property_code_key",
            "cs_set_id",
            "cs_demand_id",           
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
            {"cs_demand_id", type text},
            {"cs_start_date", type date},
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
            {"cs_demand_id", null},
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
            "cs_demand_id",
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
            {"cs_demand_id", type text},
            {"cs_start_date", type date},
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
            {"cs_demand_id", null},
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
        Web.Contents("https://docs.google.com/spreadsheets/d/19r9FdllD9Mn3zZ4dr8Ss4S5Yq2xkaIV45RudibfPUUs/export?format=csv&id=19r9FdllD9Mn3zZ4dr8Ss4S5Yq2xkaIV45RudibfPUUs&gid=1516844315"),
        [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]
    ),
  PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
  ChangedColumnTypeData = Table.TransformColumnTypes(PromotedHeaders, {{"cs_start_date", type date}, {"demand_cs_key", type text}, {"property_code", type text}, {"cs_demand_id", type text}, {"cs_set_id", type text}, {"physical_capacity", Int64.Type}, {"cs_physical_capacity", Int64.Type}, {"cs_end_date", type text}})
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
shared map_demand_channel_gsheet = let
    Source = Csv.Document(
        Web.Contents("https://docs.google.com/spreadsheets/d/1JERzvRDvVbwfdEhFwF6JtwLLJ2md-IFFd5KLnS1ZRo8/export?format=csv&id=1JERzvRDvVbwfdEhFwF6JtwLLJ2md-IFFd5KLnS1ZRo8&gid=364758502"),
        [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]
    ),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true])
in
    #"Promoted Headers";
shared SafeMapDemadChannel = let
      ExpectedColumns = {
            "channel_key", 
            "channel_group_code", 
            "channel_group", 
            "source_group_code", 
            "source_group", 
            "source_code", 
            "source", 
            "channel_sort"
      },
      //
      ActualColumns = try Table.ColumnNames(map_demand_channel_gsheet) otherwise {},
      SchemaValid = List.Difference(ExpectedColumns, ActualColumns) = {},

      SafeMapDemandChannelRaw = if SchemaValid 
      then map_demand_channel_gsheet 
      else #table(ExpectedColumns, {}),
      //
      TransformColumns = Table.TransformColumnTypes(SafeMapDemandChannelRaw, {
            {"channel_key", type text},
            {"channel_group_code", type text},
            {"channel_group", type text},
            {"source_group_code", type text},
            {"source_group", type text},
            {"source_code", type text},
            {"source", type text},
            {"channel_sort", Int64.Type}
      }),
      //
      ReplaceErrors = Table.ReplaceErrorValues(TransformColumns, {
            {"channel_key", null},
            {"channel_group_code", null},
            {"channel_group", null},
            {"source_group_code", null},
            {"source_group", type number},
            {"source_code", type number},
            {"source", type date},
            {"channel_sort", null}  
      })
in
      ReplaceErrors;
shared SafeMapDemandChannelSchemaFallback = let
  EmptyMapDemandChannelTable = #table(
  //
    {
    //
            "channel_key", 
            "channel_group_code", 
            "channel_group", 
            "source_group_code", 
            "source_group", 
            "source_code", 
            "source", 
            "channel_sort"
  //
  },
  {}
  ),
  TransformColumns = Table.TransformColumnTypes(EmptyMapDemandChannelTable, {
  //
            {"channel_key", type text},
            {"channel_group_code", type text},
            {"channel_group", type text},
            {"source_group_code", type text},
            {"source_group", type text},
            {"source_code", type text},
            {"source", type text},
            {"channel_sort", Int64.Type}
  //
  }),
  //
  ReplaceErrors = Table.ReplaceErrorValues(TransformColumns, {
  //
            {"channel_key", null},
            {"channel_group_code", null},
            {"channel_group", null},
            {"source_group_code", null},
            {"source_group", type null},
            {"source_code", type null},
            {"source", type null},
            {"channel_sort", null}  
  //
  })
in
  ReplaceErrors;
shared Demand_Channel_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "6fc7a30e-b793-4185-b9d5-2cbe481efae7"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "f266312a-87c6-47eb-aff9-fd34597688fb"]}[Data],
  TableNavigation = Navigation_2{[Id = "Demand_Channel", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
