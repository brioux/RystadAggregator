import pickle
import pandas as pd
import numpy as np

with open('rystad.panda', 'rb') as f:
    asset,approval,breakEven,OPEX,annualCapex,production,RM = pickle.load(f)

def wavg(values,weights,axis=0):
    try:
        return (values * weights).sum(axis=axis) / weights.sum(axis=axis)
    except ZeroDivisionError:
        return values.mean(axis=1)

def wstd(values,weights,axis=0):
    try:
        return (((weights*values.subtract(wavg(values,weights,axis=axis),axis=0)**2).sum(axis=axis))/
        	((weights[weights>0].count(axis=axis).subtract(1))/weights[weights>0].count(axis=axis) * weights.sum(axis=axis)))**0.5
    except ZeroDivisionError:
        return values.std(axis=1)	

def appendRows(output,df,years,i,group):
	# output:	panda DataFrame (pivot_table) to append row to
	# df:		intput DataFrame to append as pivot_table rows
	# years: 	years as column headings for pivot_table
	# i.        integer number to index new aggregate asst group
	# group 	identifier for AssetGroup (e.g. tight oil)
	assetStr = 'newAsset'+group+str(i)
	n = len(years)
	df = pd.DataFrame(data={'Value': df.values, 
		"Asset": [assetStr]*n, 
		"Country": ['Global']*n, 'Type': ['All']*n, 'Group': [group]*n, 'Year': years})
	rows= pd.pivot_table(df, values='Value', index=['Asset', 'Country', 'Type', 'Group'], columns='Year')
	return output.append(rows)

def costAggregator(output,costRange,costs,group='Other'): 
	# output: 	 panda DataFrame where the aggregate values are stored
	# costRange: cost bin ranges to aggregate over
	# costs: 	 filterd cost table to aggregate. e.g. existing assets conventional...
	# group: 	 asset group name (defaults to other). 
	
	yearRange = slice(119,200)  #take price data from year 2019 onluy
	costs = costs.iloc[:,yearRange]
	rows = costs.index.values # row index values (asset info)
	cols = costs.columns # row column values (years)
	print('aggregating asset group '+str(group))

	global production,annualCapex

	#	use production as weights
	weights = production.loc[rows,cols]
	# 	Adjust costs to account for price markers
	costs = costs.divide(RM.loc[rows,cols])

	assetGroup = []
	assetsToDrop = []
	for i in range(len(costRange)):
		if i == len(costRange)-1:
			break	

		tmp = costs[(costs>=costRange[i]) & (costs<costRange[i+1])]
		n = tmp.count()
		if n.any():
			print('aggregating '+str(costRange[i])+' to '+str(costRange[i+1]))
			# list of current assets
			assetGroup = tmp.index.values

			years = tmp.columns.values
			# weighted average of assetGroup
			tmp = wavg(tmp,weights.loc[assetGroup]) 
			output = appendRows(output,tmp,years,i,group)

			# aggregate production
			tmp = production.loc[rows,cols].sum(axis=0)
			production = appendRows(production,tmp,years,i,group)

			# aggregate annual capex
			tmp = annualCapex.loc[rows,cols].sum(axis=0)
			annualCapex = appendRows(annualCapex,tmp,years,i,group)

			#assets to drop from global list
			assetsToDrop += list(set().union(assetGroup, assetsToDrop))
			 
	production = production.drop(assetsToDrop)
	#annualCapex = annualCapex.drop(assetsToDrop)
	return output.drop(assetsToDrop)

countriesOPEC = ['Saudi Arabia','Iran','Iraq','Kuwait','Qatar','UAE','Libya',
	'Algeria','Nigeria','Equatorial Guinea',
    'Gabon','Congo','Angola','Ecuador',
    'Venezuela']

#OPEX['avg'] = wavg(_OPEX,_production,axis=1)
#OPEX['std'] = wstd(_OPEX,_production,axis=1) 
#OPEX['max'] = _OPEX.max(axis=1)
#OPEX['min'] = _OPEX.min(axis=1) 

# only projects approved before 2019
existingAssets = approval.loc[approval["Value"]<2019]['Asset'].values


#cost bins to iterate over
costRange = [0,10,30,40,50]
costRange += np.arange(51, 120, 1).tolist()

#drop OPEC
costs = OPEX.drop(index=countriesOPEC,level=1)
#drop tight oil
costs = costs.drop(index=['Tight Oil'],level=3)

# select costs for existing assets only
tmp = costs.loc[existingAssets]
OPEX = costAggregator(OPEX,costRange,tmp,'ExistConv')
for year in np.arange(2019,2050,1).tolist():
	# only projects approved after 2019
	newAssets = approval.loc[approval["Value"]==year]['Asset'].values
	# select costs for new assets 
	tmp = costs.loc[newAssets]
	OPEX = costAggregator(OPEX,costRange,tmp,'NewConv'+str(year))
	
print(OPEX)

#TO-DO
#select tight oil break even costs
#tmp = costs.select(index=['Tight Oil'],level=3)
#breakEven = costAggregator(breakEven,costRange,tmp,'ExistingConv')
