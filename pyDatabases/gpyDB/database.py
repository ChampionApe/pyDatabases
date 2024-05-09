from pyDatabases.auxfuncs import *
import gams.transfer as gt, gams
from gams.core.gmd import gmdcc
from copy import deepcopy

#global settings
gmdTypeDict = {getattr(gmdcc,f'dt_{k}'): k for k in ('set','par','var','equ','alias')} # map from integer types in gmd database to string types
default_attributes_variables = gt._internals.constants.VAR_DEFAULT_VALUES['free']
# _numtypes = (int,float,np.generic)

### --------	1. Type inference	-------- ###
def typePd(symbol, name = None, type = None, **kwargs):
	""" Series are designated as variables per default. set type = 'par' to add as parameter. 
		A set is defined as a 'subset' if symbol.name != name"""
	if isinstance(symbol, (pd.Series, pd.DataFrame)):
		return type if type else 'var'
	elif isinstance(symbol, pd.MultiIndex):
		return 'map' if symbol.nlevels>1 else set_or_subset(symbol.name, name)
	elif isinstance(symbol, pd.Index):
		return set_or_subset(symbol.name, noneInit(name, symbol.name))
	else:
		return 'scalarPar' if type in ('par', 'scalarPar') else 'scalarVar'

def set_or_subset(s_name,name):
	return 'set' if s_name == name else 'subset'

def typeGms(symbol):
	if isinstance(symbol, gt.Variable):
		return 'var' if symbol.domain_names else 'scalarVar'
	elif isinstance(symbol, gt.Parameter):
		return 'par' if symbol.domain_names else 'scalarPar'
	else:
		return typeGmsSet(symbol)

def typeGmsSet(symbol):
	return typeGmdSet(symbol.name, symbol.domain_names)

def typeGmdSet(name, domains):
	if len(domains)>1:
		return 'map'
	elif (domains in (["uni"], ["*"])) or (name == domains[0]):
		return 'set'
	else:
		return 'subset'

### -------- 	2. gpy: class of symbols    -------- ###
class gpy:
	""" Customized class of symbols used in the SeriesDB/GpyDB classes. """
	def __init__(self,symbol,**kwargs):
		""" Initialize from gmd, container, dict, or pandas object.  """
		if isinstance(symbol,(gpy,dict)):
			[setattr(self,key,value) for key,value in symbol.items() if key not in kwargs];
			[setattr(self,key,value) for key,value in kwargs.items()];
		else:
			self.vals = symbol
			self.name = kwargs['name'] if 'name' in kwargs else symbol.name
			self.type = typePd(symbol, **kwargs)
			self.text = dictInit('text',"",kwargs)

	def __iter__(self):
		return iter(self.vals)

	def __len__(self):
		return len(self.vals)

	def items(self):
		return self.__dict__.items()

	def copy(self):
		return deepcopy(self)

	@property
	def index(self):
		if isinstance(self.vals,pd.Index):
			return self.vals
		elif hasattr(self.vals,'index'):
			return self.vals.index
		else:
			return None

	@property
	def domains(self):
		return [] if self.index is None else self.index.names

	@property
	def np(self):
		return np.hstack([self.vals.values.reshape(len(self),1), 
				np.vstack([np.full(len(self), v) for k,v in self.defaultAttrs.items() if k != 'level']).T])

	@property
	def defaultAttrs(self):
		return default_attributes_variables

	@property
	def df(self):
		""" Relevant for variable types"""
		return pd.DataFrame(self.np, index = self.index, columns = default_attributes_variables.keys())

### --------	3. GAMS to pandas/gpy	-------- ###
### --------		3.1. Gmd to gpy		-------- ###
class readGmd:
	def __init__(self, db, g2np):
		self.db = db
		self.gmd = self.db._gmd
		self.g2np = g2np
		self.rc = gmdcc.new_intp()
		self.symbols = self.getSymbols()

	def __call__(self, **kwargs):
		d = {k: self.gpy(k) for k,symbol in self.symbols.items() if (self.getType(symbol) in ('var','par','set')) and (k!='SameAs')}
		d['alias_'] = self.gpy_alias(d)
		return d

	def gpy(self, name):
		attrs = self.getAttrs(self.symbols[name])
		return gpy({'name': attrs['name'], 'vals': getattr(self, f"{attrs['type']}_vals")(attrs), 'type': attrs['type'], 'text': attrs['text']})

	def gpy_alias(self, d, names = None):
		return gpy({'name': 'alias_', 'type': 'map', 'text': '', 'vals': self.getAlias(names).union([] if 'alias_' not in d else d['alias_'])})

	def getAlias(self, names):
		return pd.MultiIndex.from_tuples([self.getAliasTuple(symbol) for symbol in self.symbols.values() if self.getType(symbol) == 'alias'], names = noneInit(names, ['alias_set','alias_map2']))

	def idx_var(self, idxData, attrs):
		return self.idx_nd(idxData, attrs['domains']) if len(attrs['domains'])>1 else self.idx_1d(idxData, attrs['domains'][0]) 

	def idx_nd(self, data, domains):
		return tryIntIdx(pd.MultiIndex.from_arrays(data.T, names = domains))

	def idx_1d(self, data, domain):
		return tryIntIdx1d(pd.Index(data.reshape(data.shape[0]), name = domain))

	def var_vals(self, attrs):
		idxData, valsData = self.g2np.gmdReadSymbolStr(self.db, attrs['name'])
		return pd.Series(valsData[:,0], index = self.idx_var(idxData, attrs), name = attrs['name'])

	def par_vals(self, attrs):
		return self.var_vals(attrs)

	def scalarVar_vals(self, attrs):
		_, valsData = self.g2np.gmdReadSymbolStr(self.db, attrs['name'])
		return valsData[0,0]

	def scalarPar_vals(self, attrs):
		return self.scalarVar_vals(attrs)

	def set_vals(self, attrs):
		array = self.g2np.gmdReadSymbolStr(self.db, attrs['name'])[0]
		return self.idx_1d(array, attrs['name'])

	def subset_vals(self, attrs):
		array = self.g2np.gmdReadSymbolStr(self.db, attrs['name'])[0]
		return self.idx_1d(array, attrs['domains'][0])

	def map_vals(self, attrs):
		array = self.g2np.gmdReadSymbolStr(self.db, attrs['name'])[0]
		return self.idx_nd(array, attrs['domains'])

	def getAttrs(self, symbol):
		attrs = {'name': self.getName(symbol), 'domains': self.getDomains(symbol), 'text': self.getText(symbol), 'type': self.getType(symbol)}
		if attrs['type'] == 'set':
			attrs['type'] = typeGmdSet(attrs['name'], attrs['domains'])
		elif not attrs['domains']:
			attrs['type'] = 'scalar'+attrs['type'].capitalize()
		return attrs

	def getSymbols(self):
		return {self.getName(symbol): symbol for symbol in self.getSymbols_()}

	def getSymbols_(self):
		return (self.getSymbol(i) for i in range(gmdcc.gmdInfo(self.gmd, gmdcc.GMD_NRSYMBOLSWITHALIAS)[1]))

	def getSymbol(self, i):
		return gmdcc.gmdGetSymbolByNumberPy(self.gmd, i, self.rc)

	def addToSymbols(self, i):
		symbol = self.getSymbol(i)
		self.symbols[self.getName(symbol)] = symbol

	def getName(self, symbol):
		return gmdcc.gmdSymbolInfo(self.gmd, symbol, gmdcc.GMD_NAME)[-1]

	def getDimension(self,symbol):
		return gmdcc.gmdSymbolInfo(self.gmd, symbol, gmdcc.GMD_DIM)[1]

	def getDomains(self, symbol):
		return gmdcc.gmdGetDomain(self.gmd, symbol, self.getDimension(symbol))[-1]

	def getText(self, symbol):
		return gmdcc.gmdSymbolInfo(self.gmd, symbol, gmdcc.GMD_EXPLTEXT)[-1]

	def getType(self, symbol):
		return gmdTypeDict[gmdcc.gmdSymbolType(self.gmd, symbol)[-1]]

	def getAliasTuple(self, symbol):
		return (self.getAliasName(symbol), self.getName(symbol))

	def getAliasName(self, symbol):
		return self.getName(gmdcc.gmdGetSymbolByNumberPy(self.gmd, gmdcc.gmdSymbolInfo(self.gmd, symbol, gmdcc.GMD_USERINFO)[1]-1, self.rc))

### --------		3.2. gt.Container to gpy		-------- ###
class gpyFromGt:
	@staticmethod
	def db(container):
		d = {symbol.name: gpyFromGt.gpy(symbol) for symbol in container.getSets()+container.getParameters()+container.getVariables() if symbol.name != 'SameAs'}
		d['alias_'] = gpyFromGt.gpy_alias(container)
		return d

	@staticmethod
	def gpy(symbol):
		return gpy({'name': symbol.name, 'vals': gpyFromGt.vals(symbol), 'type': typeGms(symbol), 'text': symbol.description})

	@staticmethod
	def vals(symbol):
		if isinstance(symbol, gt.Variable):
			return gpyFromGt.var_vals(symbol)
		elif isinstance(symbol, gt.Parameter):
			return gpyFromGt.par_vals(symbol)
		elif isinstance(symbol, gt.Set):
			return gpyFromGt.set_vals(symbol)
	@staticmethod
	def set_vals(symbol):
		if symbol.records is None:
			return gpyFromGt.emptyIdx(symbol.domain_names, name = symbol.name if typeGmsSet(symbol) == 'set' else None)
		else:
			return gpyFromGt.getIdx(symbol.records, symbol.domain_labels, name = symbol.name if typeGmsSet(symbol) == 'set' else None)
	@staticmethod
	def var_vals(symbol, key = 'level'):
		if symbol.records is None:
			return pd.Series([], index = gpyFromGt.emptyIdx(symbol.domain_names)) if symbol.dimension>0 else None
		else:
			return pd.Series(symbol.records[key].values, index = gpyFromGt.getIdx(symbol.records, symbol.domain_labels)) if symbol.dimension>0 else symbol.records[key][0]
	@staticmethod
	def par_vals(symbol):
		return gpyFromGt.var_vals(symbol, key = 'value')
	@staticmethod
	def gpy_alias(container):
		return gpy({'name': 'alias_', 
					'vals': pd.MultiIndex.from_tuples([(symbol.name, symbol.alias_with.name) for symbol in container.getAliases()], names = ['alias_set','alias_map2']),
					'type': 'map',
					'text': ''})
	@staticmethod
	def emptyIdx(domain_names, name = None):
		if len(domain_names)>1:
			return pd.MultiIndex.from_tuples([], names = domain_names)
		else:
			return pd.Index([], name = name)

	@staticmethod
	def getIdx(df, domain_labels, name = None):
		if len(domain_labels)>1:
			return tryIntIdx(pd.MultiIndex.from_frame(df[domain_labels]))
		else:
			return tryIntIdx1d(pd.Index(df[domain_labels[0]], name = name))

### --------	4. From pd/gpy to GAMS	-------- ###
### --------	4.1. From pd/gpy to GAMS Container	-------- ###
class gtFromGpy:
	@staticmethod
	def add(symbol, container):
		getattr(gtFromGpy,symbol.type)(symbol,container)
	@staticmethod
	def var(symbol, container, vartype = 'free'):
		gt.Variable(container, symbol.name, type = vartype, domain = symbol.vals.index.names, records = symbol.vals.rename('level').reset_index(), description = symbol.text)
	@staticmethod
	def par(symbol, container):
		gt.Parameter(container, symbol.name, domain = symbol.vals.index.names, records = symbol.vals.rename('value').reset_index(), description = symbol.text)
	@staticmethod
	def scalarVar(symbol, container):
		gt.Variable(container, symbol.name, records = pd.DataFrame(data = [symbol.vals], columns = ['level']), description = symbol.text)
	@staticmethod
	def scalarPar(symbol, container):
		gt.Parameter(container, symbol.name, records = symbol.vals, description = symbol.text)
	@staticmethod
	def set(symbol, container):
		gt.Set(container, symbol.name, domain = symbol.vals.names if symbol.type == 'subset' else None, records = symbol.vals.values, description = symbol.text)
	@staticmethod
	def map(symbol, container):
		gt.Set(container, symbol.name, domain = symbol.vals.names, records = symbol.vals.tolist())
	@staticmethod
	def subset(symbol, container):
		return gtFromGpy.set(symbol, container)

### --------	4.2. From pd/gpy to Gmd database	-------- ###
class gmdFromGpy:
	@staticmethod
	def db(dbGpy, gmd, merge = True):
		""" Add all symbols from dbGpy to a gmd database, gmd = readGmd instance"""
		[gmdFromGpy.aom(symbol, gmd, merge = merge) for symbol in dbGpy];
	@staticmethod
	def initDb(dbGpy, gmd):
		[gmdFromGpy.add_gmd(symbol, gmd) for symbol in dbGpy];
	@staticmethod
	def aom(symbol, gmd, merge =True):
		""" add or merge"""
		if symbol.name in gmd.symbols:
			gmdFromGpy.adjust(symbol, gmd.db, gmd.g2np, merge=merge)
		else:
			gmdFromGpy.add_gmd(symbol, gmd)

	@staticmethod
	def init_gmd(symbol, gmd):
		gmdFromGpy.init(symbol, gmd.db)
		gmd.addToSymbols(len(gmd.symbols))
	@staticmethod
	def add_gmd(symbol, gmd):
		gmdFromGpy.init_gmd(symbol, gmd)
		gmdFromGpy.adjust(symbol, gmd.db, gmd.g2np, merge = False)

	@staticmethod
	def init(symbol, db):
		getattr(gmdFromGpy, f'init_{symbol.type}')(symbol, db)
	@staticmethod
	def adjust(symbol, db, g2np, merge = True):
		getattr(gmdFromGpy, f'adjust_{symbol.type}')(symbol, db, g2np, merge = merge)
	@staticmethod
	def add(symbol, db, g2np):
		gmdFromGpy.init(symbol, db)
		gmdFromGpy.adjust(symbol, db, g2np, merge = False)

	@staticmethod
	def init_set(symbol, db):
		db.add_set(symbol.name, 1, symbol.text)
	@staticmethod
	def init_subset(symbol, db):
		db.add_set_dc(symbol.name, symbol.domains, symbol.text)
	@staticmethod
	def init_map(symbol, db):
		db.add_set_dc(symbol.name, symbol.domains, symbol.text)
	@staticmethod
	def init_var(symbol, db):
		db.add_variable_dc(symbol.name, gams.VarType.Free,symbol.domains,symbol.text)
	@staticmethod
	def init_par(symbol, db):
		db.add_parameter_dc(symbol.name, symbol.domains, symbol.text)
	@staticmethod
	def init_scalarVar(symbol, db):
		db.add_variable(symbol.name, 0, gams.VarType.Free,symbol.text)
	@staticmethod
	def init_scalarPar(symbol, db):
		db.add_parameter(symbol.name, 0, symbol.text)

	@staticmethod
	def adjust_set(symbol, db, g2np, merge = True):
		g2np.gmdFillSymbolStr(db, db[symbol.name], *gmdFromGpy.gpySet2np(symbol), merge = merge)
	def adjust_subset(symbol, db, g2np, merge = True):
		gmdFromGpy.adjust_set(symbol, db, g2np, merge = merge)
	@staticmethod
	def adjust_map(symbol, db, g2np, merge = True):
		gmdFromGpy.adjust_set(symbol, db, g2np, merge = merge)
	@staticmethod
	def adjust_var(symbol, db, g2np, merge = True):
		g2np.gmdFillSymbolStr(db, db[symbol.name], *gmdFromGpy.gpyVar2np(symbol), merge = merge)
	@staticmethod
	def adjust_par(symbol, db, g2np, merge = True):
		g2np.gmdFillSymbolStr(db, db[symbol.name], *gmdFromGpy.gpyPar2np(symbol), merge = merge)
	@staticmethod
	def adjust_scalarVar(symbol, db, g2np, merge = True):
		g2np.gmdFillSymbolStr(db, db[symbol.name], *gmdFromGpy.gpyScalarVar2np(symbol), merge = merge)
	@staticmethod
	def adjust_scalarPar(symbol, db, g2np, merge = True):
		g2np.gmdFillSymbolStr(db, db[symbol.name], *gmdFromGpy.gpyScalarPar2np(symbol), merge = merge)
	@staticmethod
	def gpySet2np(s):
		return gmdFromGpy.gpyIdx2np(s.vals), gmdFromGpy.textVector(s)
	@staticmethod
	def gpyIdx2np(idx):
		return idx.to_frame(index=False).values.astype(str).astype(object)
	@staticmethod
	def textVector(s):
		""" Explanatory text accompanying index symbols; for now, pass empty vector with suitable shape."""
		return np.full((len(s),1), '', dtype = 'object')
	@staticmethod
	def gpyVar2np(s):
		return gmdFromGpy.gpyIdx2np(s.vals.index), s.np
	@staticmethod
	def gpyPar2np(s):
		return gmdFromGpy.gpyIdx2np(s.vals.index), s.vals.values.reshape(len(s),1)
	@staticmethod
	def gpyScalarVar2np(s):
		return np.empty((1,0), dtype =object), np.array([list((gt._internals.constants.VAR_DEFAULT_VALUES['free'] |{'level': s.vals}).values())])
	@staticmethod
	def gpyScalarPar2np(s):
		return np.empty((1,0), dtype =object), np.array([[s.vals]])

### --------	5. Merging methods	-------- ###

class MergeDbs:
	@staticmethod
	def merge(db, dbb, priority='second'):
		return getattr(MergeDbs, f'{db.__class__.__name__}_{dbb.__class__.__name__}')(db, dbb, priority=priority)

	@staticmethod
	def readGmd_GpyDB(gmd, db, priority='second'):
		return MergeDbs.readGmd_dict(gmd, db.series.database, priority=priority)
	@staticmethod
	def readGmd_SeriesDB(gmd, db, priority='second'):
		return MergeDbs.readGmd_dict(gmd, db.database, priority=priority)
	@staticmethod
	def readGmd_dict(gmd, db, priority = 'second'):
		partition = OrdSet.partition(OrdSet(gmd.symbols), OrdSet(db)) # split into tuple with 0 = overlap, 1 = only in gmd, 2 = only in db
		[gmdFromGpy.add_gmd(db[k], gmd) for k in partition[2]];
		[MergeSyms.merge(gmd, db[k], name = k, priority=priority) for k in partition[0]];
		return gmd
	@staticmethod
	def readGmd_readGmd(gmd, gmdd, priority = 'second'):
		db = gmdd() # read as dictionary of gpy symbols
		return MergeDbs.readGmd_dict(gmd, db, priority = priority)

	@staticmethod
	def dict_GpyDB(db, dbb, priority='second'):
		return MergeDbs.dict_dict(db, dbb.series.database, priority=priority)
	@staticmethod
	def dict_SeriesDB(db, dbb, priority='second'):
		return MergeDbs.dict_dict(db, dbb.database, priority = priority)
	@staticmethod
	def dict_dict(db, dbb, priority = 'second'):
		partition = OrdSet.partition(OrdSet(db), OrdSet(dbb)) # split into tuple with 0 = overlap, 1 = only in db, 2 = only in dbb
		db.update({k: v for k,v in dbb.items() if k in partition[2]}) # add symbols that are only in dbb
		[MergeSyms.merge(db[k], dbb[k], priority=priority) for k in partition[0]];
		return db
	@staticmethod
	def dict_readGmd(db, gmd, priority='second'):
		return MergeDbs.dict_dict(db, gmd(), priority=priority)

	@staticmethod
	def GpyDB_GpyDB(db, dbb, priority = 'second'):
		db.series.database = MergeDbs.dict_dict(db.series.database, dbb.series.database, priority=priority)
		return db
	@staticmethod
	def GpyDB_SeriesDB(db, dbb, priority='second'):
		db.series.database = MergeDbs.dict_dict(db.series.database, dbb.database, priority=priority)
		return db
	@staticmethod
	def GpyDB_dict(db, dbb, priority = 'second'):
		db.series.database = MergeDbs.dict_dict(db.series.database, dbb, priority=priority)
		return db
	@staticmethod
	def GpyDB_readGmd(db, gmd, priority='second'):
		db.series.database = MergeDbs.dict_readGmd(db.series.database, gmd, priority=priority)
		return db

	@staticmethod
	def SeriesDB_GpyDB(db, dbb, priority = 'second'):
		db.database = MergeDbs.dict_dict(db.database, dbb.series.database, priority=priority)
		return db
	@staticmethod
	def SeriesDB_SeriesDB(db, dbb, priority='second'):
		db.database = MergeDbs.dict_dict(db.database, dbb.database, priority=priority)
		return db
	@staticmethod
	def SeriesDB_dict(db, dbb, priority = 'second'):
		db.database = MergeDbs.dict_dict(db.database, dbb, priority=priority)
		return db
	@staticmethod
	def SeriesDB_readGmd(db, gmd, priority='second'):
		db.database = MergeDbs.dict_readGmd(db.database, gmd, priority=priority)
		return db


class MergeSyms:
	@staticmethod
	def merge(s, ss, name = None, priority = 'second'):
		""" Merge symbols s and ss with types ∈ {gpy, gmdRead}. If both are gmdRead instances, provide name. """
		return getattr(MergeSyms, f'{s.__class__.__name__}_{ss.__class__.__name__}')(s, ss, name = name, priority=priority)

	@staticmethod
	def readGmd_readGmd(gmd, gmdd, name = None, priority='second'):
		""" Merge values from gmdd database to gmd. name = str name of symbol. """
		return MergeSyms.readGmd_gpy(gmd, gmdd.gpy(name), name = name, priority=priority)

	@staticmethod
	def gpy_readGmd(s, gmd, name = None, priority='second'):
		""" Merge values from the gmd symbol into gpy symbol (s). gmd is the readGmd class instance. """
		return MergeSyms.gpy_gpy(s, gmd.gpy(name), priority=priority)

	@staticmethod
	def readGmd_gpy(gmd, ss, name = None, priority='second'):
		getattr(MergeSyms, f'readGmd_gpy_{priority}')(gmd, ss)

	@staticmethod
	def gpy_gpy(s, ss, name = None, priority = 'second'):
		""" Merge two gpy symbols of similar type """
		return getattr(MergeSyms, f'gpyvals_{priority}')(s, ss)

	@staticmethod
	def readGmd_gpy_first(gmd, ss):
		""" read gmd symbol to gpy, overwrite ss values to prioritize gmd symbol, then merge this adjusted gpy symbol. """
		gmdFromGpy.adjust(MergeSyms.gpy_readGmd(ss.copy(), gmd, name = ss.name), gmd.db, gmd.g2np, merge =True)

	@staticmethod
	def readGmd_gpy_second(gmd, ss):
		""" Merge gpy symbol s into database in readGmd instance. Prioritize gpy symbol values """
		gmdFromGpy.adjust(ss, gmd.db, gmd.g2np, merge = True)

	@staticmethod
	def readGmd_gpy_replace(gmd, ss):
		""" replace values in gmd symbol with those from gpy. """
		gmd.db[ss.name].clear() # drop existing values in gmd database.
		gmdFromGpy.adjust(ss, gmd.db, gmd.g2np, merge = True)

	@staticmethod
	def gpyvals_second(s, ss):
		s.vals = getattr(MergeSyms, f'vals_{s.type}')(s.vals, ss.vals)
		return s
	@staticmethod
	def gpyvals_first(s, ss):
		s.vals = getattr(MergeSyms, f'vals_{s.type}')(ss.vals, s.vals)
		return s
	@staticmethod
	def gpyvals_replace(s, ss):
		s.vals = ss.vals
		return s
	@staticmethod
	def vals_set(s, ss):
		return s.union(ss)
	@staticmethod
	def vals_subset(s, ss):
		return s.union(ss)
	@staticmethod
	def vals_map(s, ss):
		return s.union(ss)
	@staticmethod
	def vals_var(s, ss):
		return ss.combine_first(s)
	@staticmethod
	def vals_par(s, ss):
		return ss.combine_first(s)
	@staticmethod
	def vals_scalarVar(s, ss):
		return ss
	@staticmethod
	def vals_scalarPar(s, ss):
		return ss
