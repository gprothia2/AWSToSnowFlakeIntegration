class extension:
  def custom(entity,df):

    if entity == 'CUSTOMER':
      df['C_REGION'] = 'EARTH'

    if entity == 'ORDERS':
      df['O_ORDERDISCOUNT'] = df['O_ORDERDISCOUNT'].apply(lambda x: round(x, 0))

    return df
