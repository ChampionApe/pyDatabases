import itertools, numpy as np, pandas as pd
from collections.abc import Iterable
from six import string_types
_numtypes = (int,float,np.generic)
_adj_admissable_types = (pd.Index, pd.Series, pd.DataFrame)

# Content:
# 0. Small auxiliary functions
# 1. cartesianProductIndex: Creates sparse, cartesian product from iterator of indices.
# 2. OrdSet: A small class that works like an ordered set.
# 3. adj: A small class used to subset and adjust pandas-like symbols.

### -------- 	0: Small, auxiliary functions    -------- ###
def tryint(x):
	try:
		return int(x)
	except ValueError:
		return x

def ifInt(x):
	try:
		int(x)
		return True
	except ValueError:
		return False

def return_version(x,dict_):
	if x not in dict_:
		return x
	elif (x+'_0') not in dict_:
		return x+'_0'
	else:
		maxInt = max([int(y.split('_')[-1]) for y in dict_ if (y.rsplit('_',1)[0]==x and ifInt(y.split('_')[-1]))])
		return x+'_'+str(maxInt+1)

def noneInit(x,FallBackVal):
	return FallBackVal if x is None else x

def dictInit(key,df_val,kwargs):
	return kwargs[key] if key in kwargs else df_val

def is_iterable(arg):
	return isinstance(arg, Iterable) and not isinstance(arg, string_types)

def getIndex(symbol):
	""" Defaults to None if no index is defined. """
	if hasattr(symbol, 'index'):
		return symbol.index
	elif isinstance(symbol, pd.Index):
		return symbol
	elif not is_iterable(symbol):
		return None

def getValues(symbol):
	""" Defaults to the index, if no values are defined (e.g. if symbol is an index) """
	if isinstance(symbol, (pd.Series, pd.DataFrame, pd.Index)):
		return symbol
	elif hasattr(symbol,'vals'):
		return symbol.vals
	elif not is_iterable(symbol):
		return symbol

def getDomains(x):
	return [] if getIndex(x) is None else getIndex(x).names

### -------- 	1. Cartesian produt index     -------- ###
def cartesianProductIndex(indices):
	""" Return the cartesian product of pandas indices; assumes no overlap in levels of indices. """
	if any((i.empty for i in indices)):
		return pd.MultiIndex.from_tuples([], names = [n for l in indices for n in l.names]) 
	else: 
		ndarray = fastCartesianProduct([i.values for i in indices])
		return pd.MultiIndex.from_arrays(concatArrays(ndarray, indices).T, names = [n for l in indices for n in l.names])

# Auxiliary function for cartesianProductIndex
def fastCartesianProduct(arrays):
	la = len(arrays)
	L = *map(len, arrays), la
	dtype = np.result_type(*arrays)
	arr = np.empty(L, dtype=dtype)
	arrs = *itertools.accumulate(itertools.chain((arr,), itertools.repeat(0, la-1)), np.ndarray.__getitem__),
	idx = slice(None), *itertools.repeat(None, la-1)
	for i in range(la-1, 0, -1):
		arrs[i][..., i] = arrays[i][idx[:la-i]]
		arrs[i-1][1:] = arrs[i]
	arr[..., 0] = arrays[0][idx]
	return arr.reshape(-1, la)

# Auxiliary function for cartesianProductIndex
def getndarray(onedarray):
	return pd.MultiIndex.from_tuples(onedarray).to_frame(index=False).values

# Auxiliary function for cartesianProductIndex
def ndarray_or_1darray(ndarray, indices, i):
	return getndarray(ndarray[:,i]) if isinstance(indices[i], pd.MultiIndex) else ndarray[:,i:i+1]

# Auxiliary function for cartesianProductIndex
def concatArrays(ndarray, indices):
	return np.concatenate(tuple(ndarray_or_1darray(ndarray, indices, i) for i in range(len(indices))), axis=1)

class OrdSet:
	def __init__(self,i=None):
		self.v = list(dict.fromkeys(noneInit(i,[])))

	def __iter__(self):
		return iter(self.v)

	def __len__(self):
		return len(self.v)

	def __getitem__(self,item):
		return self.v[item]

	def __setitem__(self,item,value):
		self.v[item] = value

	def __add__(self,o):
		return OrdSet(self.v+list(o))

	def __sub__(self,o):
		return OrdSet([x for x in self.v if x not in o])

	def union(self,*args):
		return OrdSet(self.__add__([x for l in args for x in l]))

	def intersection(self,*args):
		return OrdSet([x for l in self.union(args) for x in l if x in self.v])

	def update(self,*args):
		self.v = self.union(*args).v

	def copy(self):
		return OrdSet(self.v.copy())

class adj:
	@staticmethod
	def rc_AdjPd(symbol, alias = None, lag = None):
		if isinstance(symbol, pd.Index):
			return adj.AdjAliasInd(adj.AdjLagInd(symbol, lag=lag), alias = alias)
		elif isinstance(symbol, pd.Series):
			return symbol.to_frame().set_index(adj.AdjAliasInd(adj.AdjLagInd(symbol.index, lag=lag), alias=alias),verify_integrity=False).iloc[:,0]
		elif isinstance(symbol, pd.DataFrame):
			return symbol.set_index(adj.AdjAliasInd(adj.AdjLagInd(symbol.index, lag=lag), alias=alias),verify_integrity=False)
		elif isinstance(symbol, _numtypes):
			return symbol
		else:
			raise TypeError(f"rc_AdjPd only uses instances {_adj_admissable_types} (and no scalars). Input was type {type(symbol)}")
	@staticmethod
	def AdjLagInd(index_,lag=None):
		if lag:
			if isinstance(index_,pd.MultiIndex):
				return index_.set_levels([index_.levels[index_.names.index(k)]+tryint(v) for k,v in lag.items()], level=lag.keys())
			elif list(index_.domains)==list(lag.keys()):
				return index_+list(lag.values())[0]
		else:
			return index_
	@staticmethod
	def AdjAliasInd(index_,alias=None):
		alias = noneInit(alias,{})
		return index_.set_names([x if x not in alias else alias[x] for x in index_.names])
	

	@staticmethod
	def rc_pd(s=None,c=None,alias=None,lag=None, pm = True, **kwargs):
		if isinstance(s,_numtypes):
			return s
		else:
			return adj.rctree_pd(s=s, c = c, alias = alias, lag = lag, pm = pm, **kwargs)
	@staticmethod
	def rc_pdInd(s=None,c=None,alias=None,lag=None,pm=True,**kwargs):
		if isinstance(s,_numtypes):
			return None
		else:
			return adj.rctree_pdInd(s=s,c=c,alias=alias,lag=lag,pm=pm,**kwargs)
	@staticmethod
	def rctree_pd(s=None,c=None,alias=None,lag =None, pm = True, **kwargs):
		a = adj.rc_AdjPd(s,alias=alias,lag=lag)
		if pm:
			return a[adj.point_pm(getIndex(a), c, pm)]
		else:
			return a[adj.point(getIndex(a) ,c)]
	@staticmethod
	def rctree_pdInd(s=None,c=None,alias=None,lag=None,pm=True,**kwargs):
		a = adj.rc_AdjPd(s,alias=alias,lag=lag)
		if pm:
			return getIndex(a)[adj.point_pm(getIndex(a), c, pm)]
		else:
			return getIndex(a)[adj.point(getIndex(a),c)]
	@staticmethod
	def point_pm(pdObj,vi,pm):
		if isinstance(vi,_adj_admissable_types):
			return adj.bool_ss_pm(pdObj,getIndex(vi),pm)
		elif isinstance(vi,dict):
			return adj.bool_ss_pm(pdObj,adj.rctree_pdInd(**vi),pm)
		elif isinstance(vi,tuple):
			return adj.rctree_tuple_pm(pdObj,vi,pm)
		elif vi is None:
			return pdObj == pdObj
	@staticmethod
	def point(pdObj, vi):
		if isinstance(vi, _adj_admissable_types):
			return adj.bool_ss(pdObj,getIndex(vi))
		elif isinstance(vi,dict):
			return adj.bool_ss(pdObj,adj.rctree_pdInd(**vi))
		elif isinstance(vi,tuple):
			return adj.rctree_tuple(pdObj,vi)
		elif vi is None:
			return pdObj == pdObj
	@staticmethod
	def rctree_tuple(pdObj,tup):
		if tup[0]=='not':
			return adj.translate_k2pd(adj.point(pdObj,tup[1]),tup[0])
		else:
			return adj.translate_k2pd([adj.point(pdObj,vi) for vi in tup[1]],tup[0])
	@staticmethod
	def rctree_tuple_pm(pdObj,tup,pm):
		if tup[0]=='not':
			return adj.translate_k2pd(adj.point_pm(pdObj,tup[1],pm),tup[0])
		else:
			return adj.translate_k2pd([adj.point_pm(pdObj,vi,pm) for vi in tup[1]],tup[0])
	@staticmethod
	def bool_ss(pdObjIndex,ssIndex):
		o,d = adj.overlap_drop(pdObjIndex,ssIndex)
		return pdObjIndex.isin([]) if len(o)<len(ssIndex.names) else pdObjIndex.droplevel(d).isin(adj.reorder(ssIndex,o))
	@staticmethod
	def bool_ss_pm(pdObjIndex,ssIndex,pm):
		o = adj.overlap_pm(pdObjIndex, ssIndex)
		if o:
			return pdObjIndex.droplevel([x for x in pdObjIndex.names if x not in o]).isin(adj.reorder(ssIndex.droplevel([x for x in ssIndex.names if x not in o]),o))
		else:
			return pdObjIndex==pdObjIndex if pm is True else pdObjIndex.isin([])
	@staticmethod
	def overlap_drop(pdObjIndex,index_):
		return [x for x in pdObjIndex.names if x in index_.names],[x for x in pdObjIndex.names if x not in index_.names]
	@staticmethod
	def overlap_pm(pdObjIndex,index_):
		return [x for x in pdObjIndex.names if x in index_.names]
	@staticmethod
	def reorder(index_,o):
		return index_ if len(index_.names)==1 else index_.reorder_levels(o)
	@staticmethod
	def translate_k2pd(l,k):
		if k == 'and':
			return sum(l)==len(l)
		elif k == 'or':
			return sum(l)>0
		elif k == 'not' and isinstance(l,(list,set)):
			return ~l[0]
		elif k == 'not':
			return ~l
