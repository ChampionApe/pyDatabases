from _mixedTools import *
from db.db import database, getIndex, getDomains, type_, _numtypes, _admissable_types
import openpyxl,io

# 1: Read methods:
class read:
	@staticmethod
	def dbFromWB(workbook, kwargs, spliton='/'):
		""" 'read' should be a dictionary with keys = method, value = list of sheets to apply this to."""
		wb = read.simpleLoad(workbook) if isinstance(workbook,str) else workbook
		db = database()
		[db.mergeDbs(getattr(read, function)(wb[sheets],spliton=spliton)) for function,sheets in kwargs.items()];
		return db

	@staticmethod
	def simpleLoad(workbook):
		with open(workbook,"rb") as file:
			in_mem_file = io.BytesIO(file.read())
		return openpyxl.load_workbook(in_mem_file,read_only=True,data_only=True)

	def sheetnames_from_wb(self, wb):
		return (sheet.title for sheet in wb._sheets)

	@staticmethod
	def sets(sheet, **kwargs):
		""" Return a dictionary with keys = set names and values = pandas objects. na entries are removed. 
			The name of each set is defined as the first entry in each column. """
		pd_sheet = pd.DataFrame(sheet.values)
		return {pd_sheet.iloc[0,i]: pd.Index(pd_sheet.iloc[1:,i].dropna(),name=pd_sheet.iloc[0,i]) for i in range(pd_sheet.shape[1])}

	@staticmethod
	def subsets(sheet,spliton='/'):
		pd_sheet = pd.DataFrame(sheet.values)
		return {pd_sheet.iloc[0,i].split(spliton)[0]: pd.Index(pd_sheet.iloc[1:,i].dropna(),name=pd_sheet.iloc[0,i].split(spliton)[1]) for i in range(pd_sheet.shape[1])}

	@staticmethod
	def aux_map(sheet,col,spliton):
		pd_temp = sheet[col]
		pd_temp.columns = [x.split(spliton)[1] for x in pd_temp.iloc[0,:]]
		return pd.MultiIndex.from_frame(pd_temp.dropna().iloc[1:,:])

	@staticmethod
	def maps(sheet,spliton='/'):
		pd_sheet = pd.DataFrame(sheet.values)
		pd_sheet.columns = [x.split(spliton)[0] for x in pd_sheet.iloc[0,:]]
		return {col: aux_map(pd_sheet,col,spliton) for col in set(pd_sheet.columns)}

	@staticmethod
	def aux_var(sheet,col,spliton):
		pd_temp = sheet[col].dropna()
		pd_temp.columns = [x.split(spliton)[1] for x in pd_temp.iloc[0,:]]
		if pd_temp.shape[1]==2:
			index = pd.Index(pd_temp.iloc[1:,0])
		else:
			index = pd.MultiIndex.from_frame(pd_temp.iloc[1:,:-1])
		return pd.Series(pd_temp.iloc[1:,-1].values,index=index,name=col)

	@staticmethod
	def variables(sheet,spliton='/'):
		pd_sheet = pd.DataFrame(sheet.values)
		pd_sheet.columns = [x.split(spliton)[0] for x in pd_sheet.iloc[0,:]]
		return {col: aux_var(pd_sheet,col,spliton) for col in set(pd_sheet.columns)}

	@staticmethod
	def scalars(sheet,**kwargs):
		pd_sheet = pd.DataFrame(sheet.values)
		return {pd_sheet.iloc[i,0]: pd_sheet.iloc[i,1] for i in range(pd_sheet.shape[0])}

	@staticmethod
	def variable2D(sheet,spliton='/',**kwargs):
		""" Read in 2d variable arranged in matrix; Note, only reads 1 variable per sheet."""
		pd_sheet = pd.DataFrame(sheet.values)
		domains = pd_sheet.iloc[0,0].split(spliton)
		var = pd.DataFrame(pd_sheet.iloc[1:,1:].values, index = pd.Index(pd_sheet.iloc[1:,0],name=domains[1]), columns = pd.Index(pd_sheet.iloc[0,1:], name = domains[2])).stack()
		var.name = domains[0]
		return {domains[0]: var}

# 2: Broadcasting-like methods
def applyMult(symbol, mapping):
	""" Apply 'mapping' to a symbol using multiindex """
	if isinstance(symbol,pd.Index):
		return (pd.Series(0, index = symbol).add(pd.Series(0, index = rc_pd(mapping,symbol)))).dropna().index.reorder_levels(symbol.names+[k for k in mapping.names if k not in symbol.names])
	elif isinstance(symbol,pd.Series):
		if symbol.empty:
			return pd.Series([], index = pd.MultiIndex.from_tuples([], names = symbol.index.names + [k for k in mapping.names if k not in symbol.index.names]))
		else: 
			return symbol.add(pd.Series(0, index = rc_pd(mapping,symbol))).reorder_levels(symbol.index.names+[k for k in mapping.names if k not in symbol.index.names])

def appendIndexWithCopy(index, copyLevel, newLevel):
	if is_iterable(copyLevel):
		return pd.MultiIndex.from_frame(index.to_frame(index=False).assign(**{newLevel[i]: index.get_level_values(copyLevel[i]) for i in range(len(copyLevel))}))
	else: 
		return pd.MultiIndex.from_frame(index.to_frame(index=False).assign(**{newLevel: index.get_level_values(copyLevel)}))

def broadcast(x,y,fill_value=0):
	""" y is a index or None, x is a scalar or series."""
	if type_(y) == 'set':
		if getDomains(y):
			if not getDomains(x):
				return pd.Series(x, index = y)
			elif set(getDomains(x)).intersection(set(getDomains(y))):
				if set(getDomains(x))-set(getDomains(y)):
					return x.add(pd.Series(0, index = y), fill_value=fill_value)
				else:
					return pd.Series(0, index=y).add(x,fill_value=fill_value)
			else:
				return pd.Series(0, index = cartesianProductIndex([database.getIndex(x),y])).add(x,fill_value=fill_value)
		else:
			return x
	else:
		b = broadcast(x, getIndex(y),fill_value=fill_value)
		return b.add(y,fill_value=fill_value) if isinstance(b,pd.Series) else x+y

# 2: Subset symbols:
def tryint(x):
	try:
		return int(x)
	except ValueError:
		return x

def rc_AdjPd(symbol, alias = None, lag = None):
	if isinstance(symbol, pd.Index):
		return AdjAliasInd(AdjLagInd(symbol, lag=lag), alias = alias)
	elif isinstance(symbol, pd.Series):
		return symbol.to_frame().set_index(AdjAliasInd(AdjLagInd(symbol.index, lag=lag), alias=alias),verify_integrity=False).iloc[:,0]
	elif isinstance(symbol, pd.DataFrame):
		return symbol.set_index(AdjAliasInd(AdjLagInd(symbol.index, lag=lag), alias=alias),verify_integrity=False)
	elif isinstance(symbol, _numtypes):
		return symbol
	else:
		raise TypeError(f"rc_AdjPd only uses instances {_admissable_types} (and no scalars). Input was type {type(symbol)}")

def AdjLagInd(index_,lag=None):
	if lag:
		if isinstance(index_,pd.MultiIndex):
			return index_.set_levels([index_.levels[index_.names.index(k)]+tryint(v) for k,v in lag.items()], level=lag.keys())
		elif list(index_.domains)==list(lag.keys()):
			return index_+list(lag.values())[0]
	else:
		return index_

def AdjAliasInd(index_,alias=None):
	alias = noneInit(alias,{})
	return index_.set_names([x if x not in alias else alias[x] for x in index_.names])

# Subsetting methods:
def rc_pd(s=None,c=None,alias=None,lag=None, pm = True, **kwargs):
	if isinstance(s,_numtypes):
		return s
	else:
		return rctree_pd(s=s, c = c, alias = alias, lag = lag, pm = pm, **kwargs)

def rc_pdInd(s=None,c=None,alias=None,lag=None,pm=True,**kwargs):
	if isinstance(s,_numtypes):
		return None
	else:
		return rctree_pdInd(s=s,c=c,alias=alias,lag=lag,pm=pm,**kwargs)

def rctree_pd(s=None,c=None,alias=None,lag =None, pm = True, **kwargs):
	adj = rc_AdjPd(s,alias=alias,lag=lag)
	if pm:
		return adj[point_pm(getIndex(adj), c, pm)]
	else:
		return adj[point(getIndex(adj) ,c)]

def rctree_pdInd(s=None,c=None,alias=None,lag=None,pm=True,**kwargs):
	adj = rc_AdjPd(s,alias=alias,lag=lag)
	if pm:
		return getIndex(adj)[point_pm(getIndex(adj), c, pm)]
	else:
		return getIndex(adj)[point(getIndex(adj),c)]

def point_pm(pdObj,vi,pm):
	if isinstance(vi,_admissable_types):
		return bool_ss_pm(pdObj,getIndex(vi),pm)
	elif isinstance(vi,dict):
		return bool_ss_pm(pdObj,rctree_pdInd(**vi),pm)
	elif isinstance(vi,tuple):
		return rctree_tuple_pm(pdObj,vi,pm)
	elif vi is None:
		return pdObj == pdObj

def point(pdObj, vi):
	if isinstance(vi, _admissable_types):
		return bool_ss(pdObj,getIndex(vi))
	elif isinstance(vi,dict):
		return bool_ss(pdObj,rctree_pdInd(**vi))
	elif isinstance(vi,tuple):
		return rctree_tuple(pdObj,vi)
	elif vi is None:
		return pdObj == pdObj

def rctree_tuple(pdObj,tup):
	if tup[0]=='not':
		return translate_k2pd(point(pdObj,tup[1]),tup[0])
	else:
		return translate_k2pd([point(pdObj,vi) for vi in tup[1]],tup[0])

def rctree_tuple_pm(pdObj,tup,pm):
	if tup[0]=='not':
		return translate_k2pd(point_pm(pdObj,tup[1],pm),tup[0])
	else:
		return translate_k2pd([point_pm(pdObj,vi,pm) for vi in tup[1]],tup[0])

def bool_ss(pdObjIndex,ssIndex):
	o,d = overlap_drop(pdObjIndex,ssIndex)
	return pdObjIndex.isin([]) if len(o)<len(ssIndex.names) else pdObjIndex.droplevel(d).isin(reorder(ssIndex,o))

def bool_ss_pm(pdObjIndex,ssIndex,pm):
	o = overlap_pm(pdObjIndex, ssIndex)
	if o:
		return pdObjIndex.droplevel([x for x in pdObjIndex.names if x not in o]).isin(reorder(ssIndex.droplevel([x for x in ssIndex.names if x not in o]),o))
	else:
		return pdObjIndex==pdObjIndex if pm is True else pdObjIndex.isin([])

def overlap_drop(pdObjIndex,index_):
	return [x for x in pdObjIndex.names if x in index_.names],[x for x in pdObjIndex.names if x not in index_.names]

def overlap_pm(pdObjIndex,index_):
	return [x for x in pdObjIndex.names if x in index_.names]

def reorder(index_,o):
	return index_ if len(index_.names)==1 else index_.reorder_levels(o)

def translate_k2pd(l,k):
	if k == 'and':
		return sum(l)==len(l)
	elif k == 'or':
		return sum(l)>0
	elif k == 'not' and isinstance(l,(list,set)):
		return ~l[0]
	elif k == 'not':
		return ~l

