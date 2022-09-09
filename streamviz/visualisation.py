import pandas as pd
import altair as alt


def check_string(string:str) -> None:
  '''When it comes to period charts, 
  it checks if the agg_level is a correct value'''
  try:
    string in ['date_hour', 'date', 'topic']
  except Exception:
    print("Introduce a valid input: 'date_hour', 'date' or 'topic'")

# EMOTION CHARTS

def get_long_emotion_df(df:pd.DataFrame, agg_level:str) -> pd.DataFrame:

  def get_long_data(df:pd.DataFrame) -> pd.DataFrame:
      agg_level = df.columns[0]
      df_emo_long = df.melt(id_vars=agg_level, value_vars=['surprise', 'fear', 'joy', 'sadness', 'anger','love'])
      df_emo_long.rename(columns={'variable':'emotion', 'value':'counts'}, inplace=True)
      df_emo_long['percent'] = df_emo_long['counts'].groupby(agg_level)['counts'].transform('sum')
      
      return df_emo_long

  check_string(agg_level)

  df_emo = df[[agg_level, 'anger', 'fear', 'joy', 'love', 'sadness', 'surprise']].groupby(agg_level).sum().reset_index()
  
  return get_long_data(df_emo)