from dataclasses import dataclass

@dataclass
class HyperFileConfig:
  """Class for changing default configurations.
      
      timestamp_with_timezone: bool, True to use timestamptz datatype with HyperFile, enable if using timestamp values with    Parquet create mode (default=False)

      allow_nulls: bool, True to skip default behavior of replacing null numeric and strings with non-null values (default=False).

      convert_decimal_precision: bool, True to automatically convert decimals with precision over 18 down to 18. This has risk of data truncation so manual testing of your decimals is suggested before use. (default=False) 
  """
  timestamp_with_timezone: bool = False
  allow_nulls: bool = False
  convert_decimal_precision: bool = False
