import itertools, numpy as np, pandas as pd
import warnings
warnings.filterwarnings("ignore", message = "The behavior of array concatenation with empty entries is deprecated. In a future version, this will no longer exclude empty items when determining the result dtype. To retain the old behavior, exclude the empty entries before the concat operation.")
from collections.abc import Iterable
from six import string_types
_numtypes = (int,float,np.generic)
_adj_admissable_types = (pd.Index, pd.Series, pd.DataFrame)

def tryIntIdx(idx):
	return idx.set_levels([tryIntIdx1d(i) for i in idx.levels])

def tryIntIdx1d(idx):
	try:
		return idx.astype(int)
	except ValueError:
		return idx

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

def domains_vlist(vlist):
	return OrdSet().union(*[OrdSet(getDomains(vi)) for vi in vlist]).v

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

def pdGb(x, by):
	if is_iterable(by):
		return x.groupby([k for k in x.index.names if k not in by])
	else:
		return x.groupby([k for k in x.index.names if k != by])
def pdSum(x,sumby):
	return pdGb(x, sumby).sum() if isinstance(x.index, pd.MultiIndex) else sum(x)

def readSetsFromDb(db, types = None):
	""" Read sets from database symbols """
	[db.addOrMerge(set_, getIndex(symbol).get_level_values(set_).unique()) for symbol in db.getTypes(noneInit(types,['var'])).values() for set_ in getIndex(symbol).names];

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

	@staticmethod
	def intersection_(s, ss):
		s.v = [x for x in s if x in ss]
		return s

	def intersection(self, *args):
		out = self.copy()
		[OrdSet.intersection_(out, x) for x in args]
		return out

	@staticmethod
	def partition(s, ss):
		overlap = OrdSet.intersection(s, ss)
		return (overlap, s-overlap, ss-overlap)

	def update(self,*args):
		self.v = self.union(*args).v

	def copy(self):
		return OrdSet(self.v.copy())

class adj:
	@staticmethod
	def rc_AdjGpy(s, c = None, alias = None, lag = None, pm = True, **kwargs):
		if c is None:
			return adj.AdjGpy(s,alias=alias, lag = lag)
		else:
			copy = s.copy()
			copy.vals = adj.rc_pd(s=s,c=c,alias=alias,lag=lag,pm=pm)
			return copy
	@staticmethod
	def AdjGpy(symbol, alias = None, lag = None):
		copy = symbol.copy()
		copy.vals = adj.rc_AdjPd(symbol.vals, alias=alias, lag = lag)
		return copy

	@staticmethod
	def rc_AdjPd(symbol, alias = None, lag = None):
		if isinstance(symbol, pd.Index):
			return adj.AdjAliasInd(adj.AdjLagInd(symbol, lag=lag), alias = alias)
		elif isinstance(symbol, pd.Series):
			return symbol.to_frame().set_index(adj.AdjAliasInd(adj.AdjLagInd(symbol.index, lag=lag), alias=alias),verify_integrity=False).iloc[:,0]
		elif isinstance(symbol, pd.DataFrame):
			return symbol.set_index(adj.AdjAliasInd(adj.AdjLagInd(symbol.index, lag=lag), alias=alias),verify_integrity=False)
		elif hasattr(symbol,'vals'):
			return adj.rc_AdjPd(symbol.vals, alias = alias, lag = lag)
		elif isinstance(symbol, _numtypes):
			return symbol
		else:
			raise TypeError(f"rc_AdjPd only uses instances {_adj_admissable_types} or gpy. Input was type {type(symbol)}")

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
		return s if isinstance(s, _numtypes) else adj.rctree_pd(s=s, c = c, alias = alias, lag = lag, pm = pm, **kwargs)

	@staticmethod
	def rc_pdInd(s=None,c=None,alias=None,lag=None,pm=True,**kwargs):
		return None if isinstance(s,_numtypes) else adj.rctree_pdInd(s=s,c=c,alias=alias,lag=lag,pm=pm,**kwargs)

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
		if isinstance(vi ,_adj_admissable_types) or hasattr(vi, 'vals'):
			return adj.bool_ss_pm(pdObj,getIndex(vi),pm)
		elif isinstance(vi,dict):
			return adj.bool_ss_pm(pdObj,adj.rctree_pdInd(**vi),pm)
		elif isinstance(vi,tuple):
			return adj.rctree_tuple_pm(pdObj,vi,pm)
		elif vi is None:
			return pdObj == pdObj
	@staticmethod
	def point(pdObj, vi):
		if isinstance(vi ,_adj_admissable_types) or hasattr(vi, 'vals'):
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

### -------- 	4: Broadcasting methods    -------- ###
class adjMultiIndex:
	@staticmethod
	def bc(x,y,fill_value = 0):
		""" Broadcast domain of 'x' to conform with domain of 'y'. """
		y, y_dom, x_dom = getIndex(y), getDomains(y), getDomains(x)
		if y_dom:
			if not x_dom:
				return pd.Series(x, index = y)
			elif set(x_dom).intersection(set(y_dom)):
				return x.add(pd.Series(0, index = y), fill_value =fill_value) if (set(x_dom)-set(y_dom)) else pd.Series(0, index = y).add(x,fill_value=fill_value)
			else:
				return pd.Series(0, index = cartesianProductIndex([getIndex(x),y])).add(x,fill_value=fill_value)
		else:
			return x

	@staticmethod
	def bcAdd(x,y,fill_value = 0):
		""" broadcast domain of 'x' to conform with domain of 'y' and add"""
		y_dom, x_dom = getDomains(y), getDomains(x)
		if y_dom:
			if not x_dom:
				return y+x
			elif set(x_dom).intersection(set(y_dom)):
				return x.add(y, fill_value = fill_value) if (set(x_dom)-set(y_dom)) else y.add(x, fill_value=fill_value)
			else:
				return pd.Series(0, index = cartesianProductIndex([getIndex(x),getIndex(y)])).add(x,fill_value=fill_value).add(y, fill_value=fill_value)
		else:
			return x+y

	@staticmethod
	def applyMultIdx(idx, mapping):
		if idx.empty:
			return pd.MultiIndex.from_tuples([], names = idx.names+ [k for k in mapping.names if k not in idx.names])
		else:
			try:
				return pd.Series(0, index = idx).add(pd.Series(0, index = adj.rc_pd(mapping, idx))).dropna().index.reorder_levels(idx.names+[k for k in mapping.names if k not in idx.names])
			except KeyError:
				return adhocFix_pandasRemovesIndexLevels(idx,mapping)

	@staticmethod
	def applyMultSrs(s, mapping):
		if s.empty:
			return pd.Series([], index = adjMultiIndex.applyMultIdx(s.index, mapping))
		else:
			return s.add(pd.Series(0, index = adj.rc_pd(mapping,s)))

	@staticmethod
	def applyMult(symbol, mapping):
		return adjMultiIndex.applyMultIdx(symbol, mapping) if isinstance(symbol, pd.Index) else adjMultiIndex.applyMultSrs(symbol, mapping)

	@staticmethod
	def grid(v0,vT,index,gridtype='linear',phi=1):
		""" If v0, vT are 1d numpy arrays, returns 2d array. If scalars, returns 1d arrays. """
		if gridtype == 'linear':
			return np.linspace(v0,vT,len(index))
		elif gridtype=='polynomial':
			return np.array([v0+(vT-v0)*((i-1)/(len(index)-1))**phi for i in range(1,len(index)+1)])

	@staticmethod
	def addGrid(v0,vT,index,name,gridtype = 'linear', phi = 1, sort_levels=None, sort_index = False):
		""" NB: Make sure that v0 and vT are sorted similarly (if they are defined over indices, that is) """
		if sort_index:
			v0 = v0.sort_index()
			vT = vT.sort_index()
		if isinstance(v0,pd.Series):
			return pd.DataFrame(adjMultiIndex.grid(v0,vT,index,gridtype=gridtype,phi=phi).T, index = v0.index, columns = index).stack().rename(name).reorder_levels(index.names+v0.index.names if sort_levels is None else sort_levels)
		else:
			return pd.Series(adjMultiIndex.grid(v0,vT,index,gridtype=gridtype,phi=phi), index = index,name=name)

def adhocFix_pandasRemovesIndexLevels(symbol, mapping):
	""" When multiindices are matched, redundant index levels are dropped automatically - this keeps them """
	s1,s2 = pd.Series(0, index = symbol), pd.Series(0, index = adj.rc_pd(mapping,symbol))
	x,y = s1.add(s2).dropna().index, s2.add(s1).dropna().index
	x_df, y_df = x.to_frame().set_index(list(set(x.names).intersection(y.names))), y.to_frame().set_index(list(set(x.names).intersection(y.names)))
	return pd.MultiIndex.from_frame(pd.concat([x_df, y_df], axis =1).reset_index())

def emptyCopy(obj):
	class Empty(obj.__class__):
		def __init__(self): pass
	newcopy = Empty()
	newcopy.__class__ = obj.__class__
	return newcopy