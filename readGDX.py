import gdxpds
import pandas as pd
import pickle

if __name__ == "__main__":
    # Loading the assets data
    
    tables = gdxpds.to_dataframes('tables.gdx')
    # Loading the regional marker
    regionalMarker = gdxpds.to_dataframes('price_ratio.gdx')
    print('Finished opening the GDX')

    # Loading the parameters from the dataFrame and setting the headers names
    # Loading sets
    asset = tables['asset']
    asset.columns = ['Asset', 'Value']
    country = tables['country']
    country.columns = ['c', 'Value']
    group = tables['group']
    group.columns = ['group', 'Value']
    assetType = tables['type']
    assetType.columns = ['type', 'Value']

    # Loading parameters
    approval = tables['approval']
    approval.columns = ['Asset', 'Value']
    start_y = tables['start']
    start_y.columns = ['Asset', 'Value']
    last_y = tables['last_year']
    last_y.columns = ['Asset', 'Value']

    breakEven = tables['breakeven']
    breakEven.columns = ['Asset', 'Country', 'Type', 'Group', 'Value']
    CAPEX_total = tables['CAPEX_total']
    CAPEX_total.columns = ['Asset', 'Country', 'Type', 'Group', 'Value']
    max_production = tables['max_production']
    max_production.columns = ['Asset', 'Country', 'Type', 'Group', 'Value']
    _annualCapex = tables['CAPEX_annual']
    _annualCapex.columns = ['Asset', 'Country', 'Type', 'Group', 'Year', 'Value']
    _OPEX = tables['OPEX_pr_bbl']
    _OPEX.columns = ['Asset', 'Country', 'Type', 'Group', 'Year', 'Value']
    _production = tables['production']
    _production.columns = ['Asset', 'Country', 'Type', 'Group', 'Year', 'Value']
    _RM = regionalMarker['price_ratio']
    _RM.columns = ['Asset', 'Country', 'Type', 'Group', 'Year', 'Value']

    # Modifying the structure of the dataFrame
    OPEX = pd.pivot_table(_OPEX, values='Value', index=['Asset', 'Country', 'Type', 'Group'], columns='Year')
    annualCapex = pd.pivot_table(_annualCapex, values='Value', index=['Asset', 'Country', 'Type', 'Group'],
                                 columns='Year')
    production = pd.pivot_table(_production, values='Value', index=['Asset', 'Country', 'Type', 'Group'],
                                columns='Year')
    RM = pd.pivot_table(_RM, values='Value', index=['Asset', 'Country', 'Type', 'Group'], columns='Year')

    del _annualCapex, _OPEX, _production, _RM

    with open('rystad.panda', 'wb') as f:
        pickle.dump([asset, country, group, assetType,
                     approval, start_y, last_y,
                     breakEven, CAPEX_total, max_production,
                     OPEX, annualCapex, production, RM], f)

    # Ready for data manipulation
    print('Data stored in pickle [asset,approval,breakEven,OPEX,annualCapex,production,RM]')

