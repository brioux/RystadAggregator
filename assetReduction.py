import pickle
import pandas as pd
import numpy as np
import gdxpds

with open('rystad.panda', 'rb') as f:
    asset, country, group, assetType, approval, start_y, last_y, breakEven, CAPEX_total, max_production, OPEX, annualCapex, production, RM = pickle.load(f)


def wavg(values, weights, axis=0):
    try:
        return (values * weights).sum(axis=axis) / weights.sum(axis=axis)
    except ZeroDivisionError:
        return values.mean(axis=1)


def wstd(values, weights, axis=0):
    try:
        return (((weights * values.subtract(wavg(values, weights, axis=axis), axis=0) ** 2).sum(axis=axis)) /
                ((weights[weights > 0].count(axis=axis).subtract(1)) / weights[weights > 0].count(
                    axis=axis) * weights.sum(axis=axis))) ** 0.5
    except ZeroDivisionError:
        return values.std(axis=1)


def appendRows(output, df, years, i, group):
    # output:	panda DataFrame (pivot_table) to append row to
    # df:		intput DataFrame to append as pivot_table rows
    # years: 	years as column headings for pivot_table
    # i.        integer number to index new aggregate asst group
    # group 	identifier for AssetGroup (e.g. tight oil)
    global approval
    assetStr = 'newAsset' + group + str(i)
    n = len(years)
    df = pd.DataFrame(data={'Value': df.values,
                            "Asset": [assetStr] * n,
                            "Country": ['Global'] * n, 'Type': ['All'] * n, 'Group': [group] * n, 'Year': years})
    rows = pd.pivot_table(df, values='Value', index=['Asset', 'Country', 'Type', 'Group'], columns='Year')

    # Adding approval_year for new assets
    if 'New' in group:
        approval = approval.append({'Asset': assetStr, 'Value': float(group[-4:])}, ignore_index=True)

    return output.append(rows)


def costAggregator(output, costRange, costs, group='Other'):
    # output: 	 panda DataFrame where the aggregate values are stored
    # costRange: cost bin ranges to aggregate over
    # costs: 	 filterd cost table to aggregate. e.g. existing assets conventional...
    # group: 	 asset group name (defaults to other).

    yearRange = slice(119, 200)  # take price data from year 2019 only
    costs = costs.iloc[:, yearRange]
    rows = costs.index.values  # row index values (asset info)
    cols = costs.columns  # row column values (years)
    print('aggregating asset group ' + str(group))

    global production, annualCapex

    # use production as weights
    weights = production.loc[rows, cols]
    # Adjust costs to account for price markers
    costs = costs.divide(RM.loc[rows, cols])

    assetsToDrop = []
    for i in range(len(costRange)):
        if i == len(costRange) - 1:
            break

        tmp = costs[(costs >= costRange[i]) & (costs < costRange[i + 1])]
        n = tmp.count()
        if n.any():
            print('aggregating ' + str(costRange[i]) + ' to ' + str(costRange[i + 1]))

            # list of current assets
            assetGroup = tmp.index.values

            years = tmp.columns.values
            # weighted average of assetGroup
            tmp = wavg(tmp, weights.loc[assetGroup])
            output = appendRows(output, tmp, years, i, group)

            # aggregate production
            try:
                # tmp = production.loc[assetGroup, cols].sum(axis=0)
                tmp = production[(costs >= costRange[i]) & (costs < costRange[i + 1])].sum(axis=0)
                tmp = tmp.iloc[yearRange]
                production = appendRows(production, tmp, years, i, group)
            except KeyError as e:
                print(e, 'In production computation')
                pass

            # aggregate annual capex
            try:
                # tmp = annualCapex.loc[assetGroup, cols].sum(axis=0)
                tmp = annualCapex[(costs >= costRange[i]) & (costs < costRange[i + 1])].sum(axis=0)
                tmp = tmp.iloc[yearRange]
                annualCapex = appendRows(annualCapex, tmp, years, i, group)
            except KeyError as e:
                print(e, 'In CAPEX computation')
                pass

            # assets to drop from global list
            assetsToDrop += list(set().union(assetGroup, assetsToDrop))

    try:
        production.drop(assetsToDrop, inplace=True)
    except KeyError as e:
        print(e, 'In dropping production assets.')
        pass
    try:
        annualCapex.drop(assetsToDrop, inplace=True)
    except KeyError as e:
        print(e, 'In dropping CAPEX data.')
        pass
    return output.drop(assetsToDrop)


countriesOPEC = ['Saudi Arabia', 'Iran', 'Iraq', 'Kuwait', 'Qatar', 'UAE', 'Libya',
                 'Algeria', 'Nigeria', 'Equatorial Guinea',
                 'Gabon', 'Congo', 'Angola', 'Ecuador',
                 'Venezuela']

# only projects approved before 2019
existingAssets = approval.loc[approval["Value"] < 2019]['Asset'].values

# cost bins to iterate over
costRange = [0, 10, 30, 40, 50]
costRange += np.arange(51, 120, 1).tolist()
# Adding the maximum mean to the list
costRange.append(OPEX.divide(RM).mean(axis=1).max())

# drop OPEC
costs = OPEX.drop(index=countriesOPEC, level=1)
# drop tight oil
costs.drop(index=['Tight Oil', 'Tight oil'], level=3, inplace=True)

# select costs for existing assets only
tmp = costs.loc[existingAssets]
OPEX = costAggregator(OPEX, costRange, tmp, 'ExistConv')
for year in np.arange(2019, 2050, 1).tolist():
    # only projects approved after 2019
    newAssets = approval.loc[approval["Value"] == year]['Asset'].values
    # select costs for new assets
    tmp = costs.loc[newAssets]
    OPEX = costAggregator(OPEX, costRange, tmp, 'NewConv' + str(year))

print(OPEX)

# TO-DO
# select tight oil break even costs
# tmp = costs.select(index=['Tight Oil'],level=3)
# breakEven = costAggregator(breakEven,costRange,tmp,'ExistingConv')


asset_list, country_list, assetType_list, group_list = zip(*OPEX.index.to_list())
asset = pd.DataFrame({'Asset': list(set(asset_list)), 'Value': True})
country = pd.DataFrame({'Asset': list(set(country_list)), 'Value': True})
assetType = pd.DataFrame({'Asset': list(set(assetType_list)), 'Value': True})
group = pd.DataFrame({'Asset': list(set(group_list)), 'Value': True})

OPEX.index.names = ['asset', 'c', 'type', 'group']
OPEX.columns.names = ['time']
production.index.names = ['asset', 'c', 'type', 'group']
production.columns.names = ['time']
annualCapex.index.names = ['asset', 'c', 'type', 'group']
annualCapex.columns.names = ['time']

max_production_GDX = production.max(axis=1).to_frame()
max_production_GDX.columns = ['Value']
max_production_GDX = max_production_GDX.reset_index()

OPEX_GDX = OPEX.stack().to_frame()
OPEX_GDX.columns = ['Value']
OPEX_GDX = OPEX_GDX.reset_index()
prod_GDX = production.stack().to_frame()
prod_GDX.columns = ['Value']
prod_GDX = prod_GDX.reset_index()
annualCapex_GDX = annualCapex.stack().to_frame()
annualCapex_GDX.columns = ['Value']
annualCapex_GDX = annualCapex_GDX.reset_index()

data_ready_for_GAMS = {'asset': asset, 'country': country, 'group': group, 'type': assetType,
                       'approval': approval, 'start': start_y, 'last_year': last_y,
                        'max_production': max_production_GDX, 'CAPEX_total': CAPEX_total, 'breakeven': breakEven,
                       'OPEX_pr_bbl':  OPEX_GDX, 'production': prod_GDX, 'CAPEX_annual': annualCapex_GDX}
gdx = gdxpds.to_gdx(data_ready_for_GAMS, path='results.gdx')
print('Results are saved to: results.gdx')
