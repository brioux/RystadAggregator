import pickle
import gdxpds
import pandas as pd
import numpy as np


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
    # group 	identifier for asssetGroup (e.g. tight oil)
    
    n = len(years)
    df = pd.DataFrame(data={'Value': df.values,
                            "Asset": [assetStr] * n,
                            "Country": [country] * n, 'Type': ['All'] * n, 'Group': [group] * n, 'Year': years})
    rows = pd.pivot_table(df, values='Value', index=['Asset', 'Country', 'Type', 'Group'], columns='Year')

    return output.append(rows)

def costAggregator(costs):
    # output: 	 panda DataFrame where the aggregate values are stored
    # costRange: cost bin ranges to aggregate over
    # costs: 	 filterd cost table to aggregate. e.g. existing assets conventional...
    
    print((" ").join(['aggregating assets for',str(country),str(group),str(asset_type),str(year)]))

    global OPEX, RM, production, annualCapex, asset_index, assetStr, approval, start_y
    assetsToDrop = []
    for i in range(len(costRange)):
        if i == len(costRange) - 1:
            break

        if asset_type=="New" :
            # return annual costs table for assets Selected by annual weigted average average 
            # (only for new assets)
            rows = costs.index  # row index values (asset info)
            tmp = costsAvg.loc[rows]
            tmp = tmp[(tmp >= costRange[i]) & (tmp < costRange[i + 1])].dropna(how='all')
#            tmp = tmp[(tmp_min>0.67*costRange[i]) & (tmp >= costRange[i]) & (tmp_max<1.33*costRange[i + 1]) & (tmp < costRange[i + 1])].dropna(how='all')

        else :
            # mask costs table for any years within range 
            # (only for existing assets)
            tmp = costs[(costs >= costRange[i]) & (costs < costRange[i + 1])]
        n = tmp.count()
        if n.any():

            #print('costs',tmp)
            print('aggregating ' + str(costRange[i]) + ' to ' + str(costRange[i + 1]))
       	    # list of current assets
            # note we use dropna(how='all') to remove empty rows, dropin assets not part of the current mask
       	    assetGroupIndex = tmp.index.get_level_values(0)
            assetGroup = assetGroupIndex.values
            assetStr = "AggregateAsset_"+str(asset_index)
       	    if asset_type=="New" :
       	    	try:
                    tmp = costs.loc[assetGroup]
       	    	except KeyError as e:
       	    		print(e, 'In selecting costs based for Annual average within range')       	    		
       	    		pass

            # list of years
            try:
                years = tmp.columns.values
            except KeyError as e:
                pass

            index = production.index.get_level_values(0).intersection(assetGroupIndex)
            # set weights to production leves for all non-null costs
            # to prevent assigning weights to null values
            weights = production.loc[index][~tmp.isnull()]  
            
            # weighted average of assetGroup
            avg = wavg(tmp, weights)
            OPEX = appendRows(OPEX, avg, years,group) 
                # Adding approval_year for new assets
             
            # weighted average of the quality index marker
            index = RM.index.get_level_values(0).intersection(assetGroupIndex)
            tmp = RM.loc[index][~tmp.isnull()]  
            avg = wavg(tmp, weights)
            RM = appendRows(RM, avg, years,group) 
                # Adding approval_year for new assets                
                
            # aggregate production
            try:
                totals = weights.sum(axis=0)
                production = appendRows(production, totals, years, group)
                totals = totals[totals>0].dropna(how='all')
                if totals.any():
                    try:
                        start_y = start_y.append({'Asset': assetStr, 'Value': int(totals.index[0])}, ignore_index=True)
                    except KeyError as e:
                        print(e, 'In start years')
            except KeyError as e:
                print(e, 'In production computation')
                pass
            
            # set approval and start year
            try:
                approval = approval.append({'Asset': assetStr, 'Value': int(year)}, ignore_index=True)
            except KeyError as e:
                print(e, 'In approval years')
            # aggregate annual capex
            try:
                index = assetGroupIndex.intersection(annualCapex.index.get_level_values(0))
                #if len(index) < len(assetGroupIndex):
                #    print(annualCapex.loc[index])
                    #print(assetGroup)

                totals = annualCapex.loc[index] 
                if asset_type != "New":
                    # only puul annual capex for non-null wieghts
                    totals = totals[~weights.isnull()]              
                totals = totals.sum(axis=0)
                annualCapex = appendRows(annualCapex, totals, years, group )
            except KeyError as e:
                print(e, 'In CAPEX computation')
                pass  
            # assets to drop from global list
            #print(assetsToDrop)
            assetsToDrop += list(set().union(assetGroup, assetsToDrop))
            asset_index += 1

    if len(assetsToDrop)>0:
        try:  	
            production.drop(assetsToDrop, inplace=True, errors='ignore' )
        except KeyError as e:
            print(e, 'In dropping production assets.')
            pass
        try:
            annualCapex.drop(assetsToDrop, inplace=True,errors='ignore' )
            #annualCapex.drop(assetsToDrop, inplace=True)
        except KeyError as e:
            print(e, 'In dropping CAPEX data.')
            pass
        try:
            OPEX.drop(assetsToDrop, inplace=True, errors='ignore' )
        except KeyError as e:
            print(e, 'In dropping OPEX data.')
            pass
         try:
            RM.drop(assetsToDrop, inplace=True, errors='ignore' )
        except KeyError as e:
            print(e, 'In dropping RM data.')
            pass
        try:    
            approval.drop(assetsToDrop, inplace=True, errors='ignore' )
        except KeyError as e:
            pass
        try:    
            start_y.drop(assetsToDrop, inplace=True, errors='ignore' )
        except KeyError as e:
            pass  
    return OPEX

def aggregate(costs):
    # sett costs aggregator for Existing and New assets
    costs = costs.dropna(how='all')
    global OPEX, asset_type, year
    year = ""
    asset_type="Existing"
    year = 2019
    tmp = costs.loc[existingAssets]
    OPEX = costAggregator(tmp)
    for year in np.arange(2020, 2051, 1).tolist():
        asset_type="New"
        # only projects approved after 2019
        #if group=='Tight Oil':
        #    # use start year for tight oil
        #    newAssets = start_y.loc[start_y["Value"] == year]['Asset'].values 
        #else :
        newAssets = approval.loc[approval["Value"] == year]['Asset'].values
        # select costs for new assets, except tight oil
        tmp = costs.loc[newAssets]
        #print(costs)
        OPEX = costAggregator(tmp)

    print('new capex',annualCapex.sum(axis=0))

countriesOtherOPEC = ['Iran', 'Iraq', 'Kuwait', 'Qatar', 'UAE', 'Libya',
                 'Algeria', 'Nigeria', 'Equatorial Guinea',
                 'Gabon', 'Congo', 'Angola', 'Ecuador',
                 'Venezuela']
countriesOPEC = countriesOtherOPEC+['Saudi Arabia']


yearRange = slice(119, 200)  # take price data starting from year 2019
production = production.iloc[:, yearRange].dropna(how='all')
OPEX = OPEX.iloc[:, yearRange].dropna(how='all')
annualCapex = annualCapex.iloc[:,yearRange].dropna(how='all')
asset_index = 0
assetStr = ''

# Set tight oil Costs to breakEven
tmp = production.query("Group == ['Tight Oil', 'Tight oil']")
indeces = tmp.index.get_level_values(0).intersection(OPEX.index.get_level_values(0))
for year in tmp.columns.values:
    OPEX.loc[indeces,year] = breakEven.loc[indeces].values
# Set gas-condensate field costs to 0m (associated production)
tmp = production.query("Type == ['Gas-Condensate field']")
indeces = tmp.index.get_level_values(0).intersection(OPEX.index.get_level_values(0))

for year in tmp.columns.values:
    OPEX.loc[indeces,year] = 0

rows = OPEX.index  # row index values (asset info)
cols = OPEX.columns  # row column values (years)
# Adjust costs to account for price markers

tmp = production.query("Group == ['Tight Oil', 'Tight oil']")
indeces = tmp.index.get_level_values(0).intersection(OPEX.index.get_level_values(0))
print(OPEX.loc[indeces, cols])
print(RM.loc[indeces, cols])
OPEX = OPEX.loc[rows, cols].divide(RM.loc[rows, cols])

print(OPEX.loc[indeces])
print(breakEven.loc[indeces])
costsAvg = wavg(OPEX,production,axis=1)
costsStd = wstd(OPEX,production,axis=1)
costsMax = OPEX.max()
costsMin = OPEX.min()
OPEX[OPEX.isnull()] = 0;
#costsAvg = costsAvg[costsStd<1]

# only projects approved before 2019
existingAssets = approval.loc[approval["Value"] <= 2019]['Asset'].values

# cost bins to iterate over
costRange = [0,10,20,30,40]
costRange += np.arange(50, 80, 1).tolist()
costRange += np.arange(90, 100, 2.5).tolist()
costRange += np.arange(100, 130, 5).tolist()
costRange += np.arange(130, 200, 10).tolist()
#costRange += np.arange(110, 150, 1).tolist()
# Adding the maximum mean to the list
costRange.append(OPEX.mean(axis=1).max())
# select costs for existing assets only

print('original capex',annualCapex.sum(axis=0))
group='All'
country = "Saudi Arabia"
costs = OPEX.query('Country == "%s"' %country)
aggregate(costs)
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

#aggregate all OTHER orginal assets with blank OPEX
group = 'ALL'
tmp = production.drop(index=["Saudi Arabia","Other OPEC","Not OPEC"], level=1)
tmp.loc[:,:] = 0
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
RM.index.names = ['asset', 'c', 'type', 'group']
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
price_ratio = RM.stack().to_frame()
price_ratio.columns = ['Value']
price_ratio = price_ratio.reset_index()
annualCapex_GDX = annualCapex.stack().to_frame()
annualCapex_GDX.columns = ['Value']
annualCapex_GDX = annualCapex_GDX.reset_index()

data_ready_for_GAMS = {'asset': asset, 'country': country, 'group': group, 'type': assetType,
                       'approval': approval, 'start': start_y, 'last_year': last_y,
                        'max_production': max_production_GDX, 'CAPEX_total': CAPEX_total, 'breakeven': breakEven_GDX,
                       'OPEX_pr_bbl':  OPEX_GDX, 'production': prod_GDX, 'CAPEX_annual': annualCapex_GDX, 'price_ratio': price_ratio}
gdx = gdxpds.to_gdx(data_ready_for_GAMS, path='results.gdx')
print('Results are saved to: results.gdx')
