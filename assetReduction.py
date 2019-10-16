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


def appendRows(output, df, years, group):
    # output:	panda DataFrame (pivot_table) to append row to
    # df:		intput DataFrame to append as pivot_table rows
    # years: 	years as column headings for pivot_table
    # i.        integer number to index new aggregate asst group
    # group 	identifier for AssetGroup (e.g. tight oil)
    global approval
    assetStr = "AggregateAsset_"+str(asset_index)
    
    n = len(years)
    df = pd.DataFrame(data={'Value': df.values,
                            "Asset": [assetStr] * n,
                            "Country": [country] * n, 'Type': ['All'] * n, 'Group': [group] * n, 'Year': years})
    rows = pd.pivot_table(df, values='Value', index=['Asset', 'Country', 'Type', 'Group'], columns='Year')

    # Adding approval_year for new assets
    if 'New' in asset_type:
        approval = approval.append({'Asset': assetStr, 'Value': year}, ignore_index=True)

    return output.append(rows)


def costAggregator(costs):
    # output: 	 panda DataFrame where the aggregate values are stored
    # costRange: cost bin ranges to aggregate over
    # costs: 	 filterd cost table to aggregate. e.g. existing assets conventional...
    rows = costs.index.values  # row index values (asset info)
    cols = costs.columns  # row column values (years)
    print((" ").join(['aggregating assets for',str(country),str(group),str(asset_type),str(year)]))

    global OPEX, production, annualCapex, asset_index

    # Adjust costs to account for price markers
    costs = costs.divide(RM.loc[rows, cols])
    assetsToDrop = []
    for i in range(len(costRange)):
        if i == len(costRange) - 1:
            break

        if asset_type=="New" :
            # return annual costs table for assets Selected by annual weigted average average 
            # (only for new assets)
            tmp = costsAvg.loc[rows]
            tmp = tmp[(tmp >= costRange[i]) & (tmp < costRange[i + 1])].dropna(how='all')
        else :
            # mask costs table for any years within range 
            # (only for existing assets)
            tmp = costs[(costs >= costRange[i]) & (costs < costRange[i + 1])].dropna(how='all')
        n = tmp.count()
        if n.any():
            print('aggregating ' + str(costRange[i]) + ' to ' + str(costRange[i + 1]))
       	    # list of current assets
            # note we use dropna(how='all') to remove empty rows, dropin assets not part of the current mask
       	    assetGroup = tmp.index.values
       	    if asset_type=="New" :
       	    	try:
       	    		tmp = costs.loc[assetGroup]
       	    	except KeyError as e:
       	    		print(e, 'In selecting costs based for Annual average within range')       	    		
       	    		pass

            # list of years
            years = tmp.columns.values
            # set weights to production leves for all non-null costs
            # to prevent assinging weights to null values
            weights = production.loc[assetGroup][~tmp.isnull()]  
            # weighted average of assetGroup
            avg = wavg(tmp, weights)
            OPEX = appendRows(OPEX, avg, years,group)  
            # aggregate production
            try:
                totals = weights.sum(axis=0)
                #tmp = production[(costs >= costRange[i]) & (costs < costRange[i + 1])].sum(axis=0)
                production = appendRows(production, totals, years, group)
            except KeyError as e:
                print(e, 'In production computation')
                pass
            # aggregate annual capex
            try:
                totals = annualCapex.loc[assetGroup][~tmp.isnull()].sum(axis=0)
                #tmp = annualCapex[(costs >= costRange[i]) & (costs < costRange[i + 1])].sum(axis=0)
                annualCapex = appendRows(annualCapex, totals, years, group )
            except KeyError as e:
                print(e, 'In CAPEX computation')
                pass  
            # assets to drop from global list
            assetsToDrop += list(set().union(assetGroup, assetsToDrop))
            asset_index += 1

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
    return OPEX.drop(assetsToDrop)

def aggregate(costs):
    # sett costs aggregator for Existing and New assets
    global OPEX, asset_type, year
    year = ""
    asset_type="Existing"
    tmp = costs.loc[existingAssets]
    OPEX = costAggregator(tmp)
    for year in np.arange(2019, 2051, 1).tolist():
        asset_type="New"
        # only projects approved after 2019
        newAssets = approval.loc[approval["Value"] == year]['Asset'].values
        # select costs for new assets, except tight oil
        tmp = costs.loc[newAssets]
        #print(costs)
        OPEX = costAggregator(tmp)

countriesOtherOPEC = ['Iran', 'Iraq', 'Kuwait', 'Qatar', 'UAE', 'Libya',
                 'Algeria', 'Nigeria', 'Equatorial Guinea',
                 'Gabon', 'Congo', 'Angola', 'Ecuador',
                 'Venezuela']
countriesOPEC = countriesOtherOPEC+['Saudi Arabia']


yearRange = slice(119, 200)  # take price data starting from year 2019
production = production.iloc[:, yearRange].dropna(how='all')
OPEX = OPEX.iloc[:, yearRange].dropna(how='all')
annualCapex = annualCapex.iloc[:,yearRange].dropna(how='all')

asset_index = 0;

OPEX[OPEX.isnull()][production>0]=0.0;

# Set tight oil Costs to breakEven
tmp = OPEX.query("Group == ['Tight Oil', 'Tight oil']")
indeces = tmp.index.values
for year in tmp.columns.values:
    OPEX.loc[indeces,year] = breakEven.loc[indeces].values
costsAvg = wavg(OPEX,production,axis=1)
costsStd = wstd(OPEX,production,axis=1)
#costsAvg = costsAvg[costsStd<1]

# only projects approved before 2019
existingAssets = approval.loc[approval["Value"] < 2019]['Asset'].values

# cost bins to iterate over
costRange = [0, 30, 40, 50]
costRange += np.arange(51, 120, 1).tolist()
# Adding the maximum mean to the list
costRange.append(OPEX.divide(RM).mean(axis=1).max())
# select costs for existing assets only


group='All'
country = "Saudi Arabia"
costs = OPEX.query('Country == "%s"' %country)
#aggregate(costs)
country = "Other OPEC"
costs = OPEX.query('Country == @countriesOtherOPEC')
aggregate(costs)


# drop OPEC
costs = OPEX.drop(index=countriesOPEC, level=1)
country = "Not OPEC"

tmp = costs.drop(index=['Tight Oil', 'Tight oil'], level=3)
group = 'Other'
aggregate(tmp)

tmp = costs.query("Group == ['Tight Oil', 'Tight oil']")
group='Tight Oil'
aggregate(tmp)

asset_list, country_list, assetType_list, group_list = zip(*production.index.to_list())
asset = pd.DataFrame({'Asset': list(set(asset_list)), 'Value': True})
country = pd.DataFrame({'Asset': list(set(country_list)), 'Value': True})
assetType = pd.DataFrame({'Asset': list(set(assetType_list)), 'Value': True})
group = pd.DataFrame({'Asset': list(set(group_list)), 'Value': True})

breakEven.index.names = ['asset', 'c', 'type', 'group']
OPEX.index.names = ['asset', 'c', 'type', 'group']
OPEX.columns.names = ['time']
production.index.names = ['asset', 'c', 'type', 'group']
production.columns.names = ['time']
annualCapex.index.names = ['asset', 'c', 'type', 'group']
annualCapex.columns.names = ['time']

max_production_GDX = production.max(axis=1).to_frame()
max_production_GDX.columns = ['Value']
max_production_GDX = max_production_GDX.reset_index()

breakEven_GDX = breakEven.stack().to_frame()
breakEven_GDX = breakEven_GDX.reset_index()
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
                        'max_production': max_production_GDX, 'CAPEX_total': CAPEX_total, 'breakeven': breakEven_GDX,
                       'OPEX_pr_bbl':  OPEX_GDX, 'production': prod_GDX, 'CAPEX_annual': annualCapex_GDX}
gdx = gdxpds.to_gdx(data_ready_for_GAMS, path='results.gdx')
print('Results are saved to: results.gdx')
