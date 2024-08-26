from .database import *
from gams.core.numpy import gams2numpy
import os, pickle, openpyxl, io

admissable_gpy_types = (pd.Series,pd.Index,int,float,str,np.generic,dict,gpy)
g2np_ = gams2numpy.Gams2Numpy()
dropattrs_ = ['database','ws','g2np','gmd']

def sunion_empty(ls):
	""" return empty set if the list of sets (ls) is empty"""
	try:
		return set.union(*ls)
	except TypeError:
		return set()

def versionizeName(name, d):
	""" If name is already used in 'd' - then try versionized name (adding +1.)"""
	if name not in d:
		return name
	else:
		partitions = [int(x.rpartition(name+'__v')[-1]) for x in d if ifInt(x.rpartition(name+'__v')[-1])]
		return name+'__v'+str(max(partitions)+1) if partitions else name+'__v1'

def partitionDoms(s, ss):
	return OrdSet.partition(OrdSet(getDomains(s)), OrdSet(getDomains(ss)))

class GpyDB:
	###################################################################################################
	###									0: Pickling/load/export settings 							###
	###################################################################################################
	def __init__(self, obj = None, ws = None, alias = None, **kwargs):
		getattr(self, f'init_{obj.__class__.__name__}')(obj, ws = ws, **kwargs)
		self.updateAlias(alias=alias)

	def init_NoneType(self, noneObj, db = None, **kwargs):
		getattr(self, f'initFromDb_{db.__class__.__name__}')(db, **kwargs)
	def init_str(self, pickle_path, ws = None, name = None):
		with open(pickle_path, "rb") as file:
			p = pickle.load(file)
		if name:
			p.name = name # update name
		if hasattr(p, '_fast'):
			self.__dict__ = p.__dict__
		else:
			p.g2np = g2np_
			p.database = None if 'database' in p.dropattrs else os.path.join(p.data_folder, f'{p.name}.gdx')
			self.init_dict(p.__dict__, ws = noneInit(ws, p.work_folder))
	def init_GpyDB(self, db, ws = None):
		d = {k: v if k in dropattrs_ else deepcopy(v) for k,v in db.__dict__.items()} # copy all attributes
		self.init_dict(d, ws = noneInit(ws, db.ws))

	def init_dict(self, d, ws = None):
		self.__dict__ = d
		if ws is not None:
			self.setWs(ws)
			self.name = versionizeName(self.name, self.ws._gams_databases)
			self.initDb(d['database'])

	def init_GamsDatabase(self, db, ws = None, data_folder = None, dropattrs = None):
		self.database = db
		self.setWs(db.workspace)
		self.name = db.name
		self.g2np = g2np_
		self.data_folder = noneInit(data_folder, os.getcwd())
		self.dropattrs = noneInit(dropattrs, dropattrs_.copy())
		self.gmd = readGmd(self.database, self.g2np)
		self.series = SeriesDB(database = self.gmd())

	def initFromDb_SeriesDB(self, series, **kwargs):
		self.initFromDb_dict(series.database, **kwargs)
	def initFromDb_dict(self, d, ws = None, name = "db", data_folder = None, dropattrs = None):
		self.setWs(ws)
		self.series = SeriesDB(database = d)
		self.name = versionizeName(name, self.ws._gams_databases)
		self.g2np = g2np_
		self.data_folder = noneInit(data_folder, os.getcwd())
		self.dropattrs = noneInit(dropattrs, dropattrs_.copy())
		self.initDb(None)
	def initFromDb_NoneType(self, db, **kwargs):
		self.initFromDb_str(db, **kwargs)
	def initFromDb_GamsDatabase(self, db, **kwargs):
		self.initFromDb_str(db, **kwargs)
	def initFromDb_str(self, db, ws = None, name = "db", data_folder = None, dropattrs = None):
		self.setWs(ws)
		self.name = versionizeName(name, self.ws._gams_databases)
		self.g2np = g2np_
		self.data_folder = noneInit(data_folder, os.getcwd())
		self.dropattrs = noneInit(dropattrs, dropattrs_.copy())
		self.initDb(db)
		self.series = SeriesDB(database = self.gmd())

	@staticmethod
	def initFast(name = "db", series = None, data_folder = None, dropattrs = None, alias = None):
		""" Initialize class without the gams related attributes """
		class Empty:
			def __init__(self): pass
		obj = Empty()
		obj.__class__ = GpyDB
		obj._fast = True 
		obj.name = name
		obj.data_folder = noneInit(data_folder, os.getcwd())
		obj.dropattrs = noneInit(dropattrs, dropattrs_.copy())
		obj.series = noneInit(series, SeriesDB())
		obj.updateAlias(alias = alias)
		return obj

	def setWs(self, ws):
		self.ws = self.initWs(ws)
		self.work_folder = self.ws.working_directory
	@staticmethod
	def initWs(ws):
		return getattr(GpyDB, f'initWs_{ws.__class__.__name__}')(ws)
	@staticmethod
	def initWs_str(ws):
		return gams.GamsWorkspace(working_directory=ws)
	@staticmethod
	def initWs_NoneType(ws):
		return gams.GamsWorkspace()
	@staticmethod
	def initWs_GamsWorkspace(ws):
		return ws

	def initDb(self, db):
		getattr(self, f'initDb_{db.__class__.__name__}')(db)
		self.gmd = readGmd(self.database, self.g2np)
		return self.database
	def initDb_NoneType(self, db):
		self.database = self.ws.add_database(database_name=self.name)
	def initDb_str(self, db):
		self.database = self.ws.add_database_from_gdx(db, database_name=self.name)
	def initDb_GamsDatabase(self, db):
		self.database = self.ws.add_database(source_database = db, database_name = self.name)
	def initDb_GpyDB(self, db):
		self.database = self.ws.add_database(source_database=db.database, database_name=self.name)

	def readAlias(self, alias = None):
		if isinstance(alias, pd.MultiIndex):
			return alias.set_names(['alias_set','alias_map2'])
		else:
			if alias is None:
				alias = []
			return pd.MultiIndex.from_tuples(alias, names = ['alias_set','alias_map2'])

	def updateAlias(self,alias=None):
		alias = self.readAlias(alias = alias)
		if 'alias_' not in self.series.database:
			self.series['alias_'] = alias
		else:
			self.series['alias_'] = self('alias_').union(alias)
		self.series['alias_set'] = self('alias_').get_level_values('alias_set').unique()
		self.series['alias_map2'] = self('alias_').get_level_values('alias_map2').unique()

	def __getstate__(self):
		if 'database' not in self.dropattrs:
			self.database.export(os.path.join(self.data_folder,self.name))
		return {key: value for key,value in self.__dict__.items() if key not in (self.dropattrs+['database'])}

	def __setstate__(self,dict_):
		""" Don't include ws. Don't include db. """
		self.__dict__ = dict_

	def export(self,name=None,repo=None):
		name = self.name if name is None else name
		repo = self.data_folder if repo is None else repo
		with open(os.path.join(repo,name), "wb") as file:
			pickle.dump(self,file)

	def copy(self, ws = None, **kwargs):
		return GpyDB(obj = self, ws = ws, **kwargs)

	###################################################################################################
	###								1: Properties and base methods 									###
	###################################################################################################

	def __iter__(self):
		return self.series.__iter__()

	def __len__(self):
		return self.series.__len__()

	def __getitem__(self,item):
		try:
			return self.series[item]
		except KeyError:
			return self.series[self.alias(item)]

	def __setitem__(self,name,value):
		self.series.__setitem__(name,value)

	def __call__(self, item):
		try:
			return self.series[item].vals
		except KeyError:
			return self.series[self.alias(item)].vals.rename(item)

	@property
	def symbols(self):
		return self.series.database

	def getTypes(self,types):
		return {symbol.name: symbol for symbol in self.series if symbol.type in types}


	###################################################################################################
	###									2: Dealing with aliases			 							###
	###################################################################################################

	@property
	def aliasDict(self):
		return {name: self('alias_').get_level_values(1)[self('alias_').get_level_values(0)==name] for name in self('alias_set')}

	@property
	def aliasDict0(self):
		return {key: self.aliasDict[key].insert(0,key) for key in self.aliasDict}

	@property
	def alias_notin_db(self):
		return set(self('alias_map2'))-set(self.getTypes(['set']))

	def aliasAll(self,x):
		if x in self('alias_set').union(self('alias_map2')):
			return self.aliasDict0[self.alias(x)]
		else: 
			return [x]

	def alias(self,x,idx=0):
		if x in self('alias_set'):
			return self.aliasDict0[x][idx]
		elif x in self('alias_map2'):
			key = self('alias_').get_level_values(0)[self('alias_').get_level_values(1)==x][0]
			return self.aliasDict0[key][idx]
		elif x in self.getTypes(['set','subset','map']) and idx==0:
			return x
		else:
			raise TypeError(f"{x} is not aliased")

	def domainsUnique(self,x):
		""" Returns list of sets a symbol x is defined over. If x is defined over a set and its alias, only the set is returned. """
		return np.unique([self.alias(name) for name in self[x].index.names]).tolist()

	def allDoms(self, types = None):
		return OrdSet.union(*(OrdSet(k.domains) for k in self.getTypes(noneInit(types, ['set','subset','map','par','var'])).values()))

	def varDom(self,set_,types=None):
		""" Returns a dict with keys = set_+aliases, values = list of symbols in 'types' defined over the the relevant set/alias"""
		return {set_i: [k for k,v in self.getTypes(noneInit(types,['var','par'])).items() if set_i in v.domains] for set_i in self.aliasAll(set_)}

	def mergeInternal(self, priority='second', name = None):
		if priority == 'replace':
			self.name = versionizeName(noneInit(name, self.name), self.ws._gams_databases)
			self.database = self.ws.add_database(database_name=self.name)
			self.gmd = readGmd(self.database, self.g2np)
			gmdFromGpy.initDb(self.series, self.gmd)
		else:
			MergeDbs.readGmd_dict(self.gmd, self.series.database, priority = priority)

	def aom(self, symbol, priority='second',**kwargs):
		self.aom_gpy(gpy(symbol, **kwargs), priority=priority)

	def aom_gpy(self, symbol, priority = 'second'):
		if symbol.name in self.series.database:
			MergeSyms.gpy_gpy(self[symbol.name], symbol, priority=priority)
		else:
			self[symbol.name] = symbol

class SeriesDB:
	""" A simple dict-like database """
	def __init__(self,database=None):
		self.database = noneInit(database,{})

	def __iter__(self):
		return iter(self.database.values())

	def __len__(self):
		return len(self.database)

	def __getitem__(self,item):
		return self.database[item]

	def __setitem__(self,item,value):
		if item in self.database:
			if not is_iterable(value) and is_iterable(self[item].vals):
				value = pd.Series(value,index=self[item].index,name=self[item].name)
		self.database[item] = gpy(value,**{'name': item})

	def __delitem__(self,item):
		del(self.database[item])

	def __call__(self, item):
		return self.database[item].vals

	def copy(self):
		return deepcopy(self)

	@property
	def symbols(self):
		return self.database

class DbFromExcel:
	@staticmethod
	def dbFromWB(workbook, kwargs, spliton='/'):
		""" kwargs is a dict with key = method, value = sheet/list of sheets"""
		""" 'read' should be a dictionary with keys = method, value = list of sheets to apply this to."""
		wb = DbFromExcel.simpleLoad(workbook) if isinstance(workbook,str) else workbook
		db = {}
		[MergeDbs.merge(db, DbFromExcel.readKwarg(k, v, wb, spliton=spliton)) for k,v in kwargs.items()];
		return db

	@staticmethod
	def readKwarg(key, value, wb, spliton = '/'):
		if type(value) is str:
			return getattr(DbFromExcel, key)(wb[value], spliton=spliton)
		elif is_iterable:
			db = {}
			[MergeDbs.merge(db, DbFromExcel.readKwarg(key, v, wb, spliton=spliton)) for v in value];
			return db

	@staticmethod
	def simpleLoad(workbook):
		with open(workbook,"rb") as file:
			in_mem_file = io.BytesIO(file.read())
		return openpyxl.load_workbook(in_mem_file,read_only=True,data_only=True)

	@staticmethod
	def sheetnames_from_wb(wb):
		return (sheet.title for sheet in wb._sheets)

	@staticmethod
	def set(sheet, **kwargs):
		""" Return a dictionary with keys = set names and values = pandas objects. na entries are removed. 
			The name of each set is defined as the first entry in each column. """
		pd_sheet = pd.DataFrame(sheet.values)
		return {pd_sheet.iloc[0,i]: gpy(pd.Index(pd_sheet.iloc[1:,i].dropna(),name=pd_sheet.iloc[0,i])) for i in range(pd_sheet.shape[1])}

	@staticmethod
	def subset(sheet,spliton='/'):
		pd_sheet = pd.DataFrame(sheet.values)
		return {pd_sheet.iloc[0,i].split(spliton)[0]: gpy(pd.Index(pd_sheet.iloc[1:,i].dropna(),name=pd_sheet.iloc[0,i].split(spliton)[1])) for i in range(pd_sheet.shape[1])}

	@staticmethod
	def aux_map(sheet,col,spliton):
		pd_temp = sheet[col]
		pd_temp.columns = [x.split(spliton)[1] for x in pd_temp.iloc[0,:]]
		return pd.MultiIndex.from_frame(pd_temp.dropna().iloc[1:,:])

	@staticmethod
	def map(sheet,spliton='/'):
		pd_sheet = pd.DataFrame(sheet.values)
		pd_sheet.columns = [x.split(spliton)[0] for x in pd_sheet.iloc[0,:]]
		return {col: gpy(DbFromExcel.aux_map(pd_sheet,col,spliton)) for col in set(pd_sheet.columns)}

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
	def var(sheet,spliton='/'):
		pd_sheet = pd.DataFrame(sheet.values)
		pd_sheet.columns = [x.split(spliton)[0] for x in pd_sheet.iloc[0,:]]
		return {col: gpy(DbFromExcel.aux_var(pd_sheet,col,spliton)) for col in set(pd_sheet.columns)}

	@staticmethod
	def par(sheet,spliton='/'):
		pd_sheet = pd.DataFrame(sheet.values)
		pd_sheet.columns = [x.split(spliton)[0] for x in pd_sheet.iloc[0,:]]
		return {col: gpy(DbFromExcel.aux_var(pd_sheet,col,spliton), type = 'par') for col in set(pd_sheet.columns)}

	@staticmethod
	def scalarVar(sheet,**kwargs):
		pd_sheet = pd.DataFrame(sheet.values)
		return {pd_sheet.iloc[i,0]: gpy(pd_sheet.iloc[i,1], name = pd_sheet.iloc[i,0]) for i in range(pd_sheet.shape[0])}

	@staticmethod
	def scalarPar(sheet,**kwargs):
		pd_sheet = pd.DataFrame(sheet.values)
		return {pd_sheet.iloc[i,0]: gpy(pd_sheet.iloc[i,1], name = pd_sheet.iloc[i,0], type = 'scalarPar') for i in range(pd_sheet.shape[0])}

	@staticmethod
	def var2D(sheet,spliton='/',**kwargs):
		""" Read in 2d variable arranged in matrix; Note, only reads 1 variable per sheet."""
		pd_sheet = pd.DataFrame(sheet.values)
		domains = pd_sheet.iloc[0,0].split(spliton)
		var = pd.DataFrame(pd_sheet.iloc[1:,1:].values, index = pd.Index(pd_sheet.iloc[1:,0],name=domains[1]), columns = pd.Index(pd_sheet.iloc[0,1:], name = domains[2])).stack()
		var.name = domains[0]
		return {domains[0]: gpy(var)}

class AggDB:
	# Main methods:
	# 1. updSetElements: Update name of set elements in database using a mapping (dictionary)
	# 2. updSetNames: Update name of a set in database.
	# 3. readSets: Add sets to the database by reading established variables/parameters/mappings.
	# 4. updSetsFromSyms: For existing database clean up set definitions and use 'readSets' method to redefine sets.
	# 5. subsetDB: Subset all symbols in database. 
	# 6. aggDB: Aggregate database according to mapping. 
	def updSetElements(db, setName, ns, rul=False):
		full_map = {k:k if k not in ns else ns[k] for k in db[setName]}
		if rul:
			AggDB.remove_unused_levels(db)
		for k,v in db.varDom(setName,types=['set','subset','map','par','var']).items():
			[AggDB.updateSetValue_sym(db[s], k, full_map) for s in v];
		return db

	def remove_unused_levels(db):
		[db[k].__setattr__('vals',db(k).remove_unused_levels()) for k in db.getTypes(['map'])];
		[db(k).__setattr__('index', db(k).index.remove_unused_levels()) for k in db.getTypes(['par','var']) if isinstance(db[k].index, pd.MultiIndex)];
	
	def updateSetValue_sym(symbol, setName, ns):
		return getattr(AggDB, f'updateSetValue_{symbol.type}')(symbol, setName, ns)
	def updateSetValue_set(symbol, setName, ns):
		symbol.vals = AggDB.updateIdxValues(symbol.vals, ns).unique()
		return symbol
	def updateSetValue_subset(symbol, setName, ns):
		symbol.vals = AggDB.updateIdxValues(symbol.vals, ns).unique()
		return symbol
	def updateSetValue_map(symbol, setName, ns):
		symbol.vals = AggDB.uniqueMIdx(AggDB.updateMIdxValues(symbol.vals, setName, ns))
		return symbol
	def updateSetValue_var(symbol, setName, ns):
		symbol.vals.index = AggDB.updateMIdxValues(symbol.index, setName, ns) if isinstance(symbol.index, pd.MultiIndex) else AggDB.updateIdxValues(symbol.index, setName, ns)
		return symbol
	def updateSetValue_par(symbol, setName, ns):
		return AggDB.updateSetValue_var(symbol, setName, ns)	
	def updateIdxValues(idx, ns, unique = True):
		return idx.map(ns)
	def updateMIdxValues(idx, setName, ns):
		return idx.set_levels(idx.levels[idx.names.index(setName)].map(ns), level = setName, verify_integrity = False)
	def uniqueMIdx(idx):
		return pd.MultiIndex.from_tuples(np.unique(idx.values), names = idx.names)

	# ----------------------- 2. Rename set names ------------------------- #
	def updSetNames(db, ns):
		""" 'ns' is a dictionary with key = original set, value = new set name. This does not alter aliases (unless they are included in 'ns') """
		[AggDB.updSetName(db,k,v) for k,v in ns.items()];
		return db
	
	def updSetName(db,k,v):
		if k in db.series.database:
			db[k].vals = db(k).rename(v)
			db.series.__delitem__(k)
		[db(vi).__setattr__('index',AggDB.renameIdx(db[vi].index,k,v)) for vi in db.varDom(k,types=('var','par'))[k]];
		[db[vi].__setattr__('vals', AggDB.renameIdx(db(vi), k ,v)) for vi in db.varDom(k,types=['map'])[k]];
		return db

	def renameIdx(idx, k, v):
		return idx.rename({k:v}) if isinstance(idx, pd.MultiIndex) else idx.rename(v)

	# ----------------------- 3-4. Read sets/update sets from database  ------------------------- #
	def updSetsFromSyms(db, types = None, clean = True, ignore_alias = True, clean_alias = False):
		if clean:
			AggDB.cleanSets(db)
		AggDB.readSets(db ,types = types, ignore_alias=ignore_alias)
		if clean_alias:
			AggDB.cleanAliases(db, types)
		AggDB.readAliasedSets(db, ignore_alias)
		if clean:
			AggDB.updateSubsetsFromSets(db)
			AggDB.updateMapsFromSets(db)
		return db

	def readSets(db, types=None, ignore_alias=True):
		""" read and define set elements from all symbols of type 'types'. """
		if ignore_alias:
			[db.aom(gpy(symbol.index.get_level_values(setName).unique().rename(db.aliasAll(setName)[0]))) for symbol in db.getTypes(noneInit(types,['var','par'])).values() for setName in set(symbol.domains)];
		else:
			[db.aom(gpy(symbol.index.get_level_values(setName).unique())) for symbol in db.getTypes(noneInit(types,['var','par'])).values() for setName in set(symbol.domains)];

	def cleanSets(db):
		""" create empty indices for all sets  """
		[db.__setitem__(set_, pd.Index([], name = set_)) for set_ in set(db.getTypes(['set']))-set(['alias_set','alias_map2'])];

	def cleanAliases(db,types):
		""" Remove aliases that are not used in variables/parameters """
		db.series['alias_'] = pd.MultiIndex.from_tuples(AggDB.activeAliases(db,types), names = ['alias_set','alias_map2'])
		db.updateAlias()

	def activeAliases(db,types):
		""" Return list of tuples with alias_ that are used in the model variables / mappings"""
		return [(k,v) for k in db('alias_set') for v in [x for x in db.aliasDict[k] if len(db.varDom(k,types=types)[x])>0]]

	def readAliasedSets(db,ignore_alias):
		""" Read in all elements for aliased sets. If ignore alias"""
		for set_i in db.aliasDict:
			all_elements = sunion_empty([set(db(set_ij)) for set_ij in db.aliasDict0[set_i] if set_ij in db.getTypes(['set'])])
			if ignore_alias:
				[db.__setitem__(set_ij, pd.Index(all_elements,name=set_ij)) for set_ij in db.aliasDict0[set_i] if set_ij in db.getTypes(['set'])];
			else:
				[db.__setitem__(set_ij, pd.Index(all_elements,name=set_ij)) for set_ij in db.aliasDict0[set_i]];

	def updateSubsetsFromSets(db):
		[AggDB.updateSubset(db,ss) for ss in db.getTypes(['subset'])];

	def updateSubset(db,ss):
		if db.alias(db(ss).name) not in db.symbols:
			db.__setitem__(ss,pd.Index([],name=db.alias(db(ss).name)))
		else:
			db.__setitem__(ss,adj.rctree_pd(s=db[ss],c=db[db.alias(db(ss).name)]))
	
	def updateMapsFromSets(db):
		[AggDB.updateMap(db,m) for m in db.getTypes(['map'])];
	
	def updateMap(db,m):
		if sum([bool(set(db.series.database.keys()).intersection(db.aliasAll(s))) for s in db[m].domains])<len(db[m].domains):
			db.__setitem__(m, pd.MultiIndex.from_tuples([], names = db[m].domains))
		else:
			db.__setitem__(m, adj.rctree_pd(s=db[m], c = ('and', [db[s] for s in db[m].domains])))

	# ----------------------- 5. Subset database with index ------------------------- #
	def subsetDB(db,index):
		[AggDB.subsetDB_valsFromList(db, index.rename(k), v) for k,v in db.varDom(index.name, types = ('set','subset','map','var','par')).items()];
		return db

	def subsetDB_valsFromList(db,index,listOfSymbols):
		[db[symbol].__setattr__('vals', adj.rctree_pd(db(symbol), index)) for symbol in listOfSymbols];

	# ----------------------- 6. Methods for aggregating database ------------------------- #
	def aggDB(db, mapping, aggBy=None, replaceWith=None, aggLike = None):
		""" Aggregate symbols in db according to mapping. This does so inplace, i.e. the set aggBy is altered. 
			Note: The aggregation assumes that mapping is 'one-to-many'; if this is not the case, a warning is printed (if checkUnique) """
		aggBy,replaceWith = noneInit(aggBy, mapping.names[0]), noneInit(replaceWith,mapping.names[-1])
		defaultAggLike = {k: {'func': 'Sum', 'kwargs': {}} for v,l in db.varDom(aggBy, types = ['var','par']).items() for k in l}
		aggLike = defaultAggLike if aggLike is None else defaultAggLike | aggLike
		[db.__setitem__(k, AggDB.aggDB_set(k, mapping, aggBy, replaceWith)) for k in set(db.aliasDict0[aggBy])-db.alias_notin_db]; # this alters sets and potentially aliases if they are also defined in the database
		[db.__setitem__(vi, AggDB.aggDB_subset(db, vi, mapping.set_names(k,level=aggBy), k, replaceWith)) for k,v in db.varDom(aggBy, types=['subset']).items() for vi in v];
		[db.__setitem__(vi, AggDB.aggDB_mapping(db, vi, mapping.set_names(k,level=aggBy), k, replaceWith)) for k,v in db.varDom(aggBy, types=['map']).items() for vi in v];
		[db.__setitem__(vi, getattr(AggDB,f"aggVar{aggLike[vi]['func']}")(db(vi), mapping.set_names(k,level=aggBy),k,replaceWith,**aggLike[vi]['kwargs'])) for k,v in db.varDom(aggBy).items() for vi in v];
		return db

	def aggDB_set(k, mapping, aggBy, replaceWith):
		return mapping.get_level_values(replaceWith).unique().rename(k)
	
	def aggDB_subset(db, k, mapping, aggBy, replaceWith):
		o, d1, d2 = partitionDoms(db[k], mapping)
		return AggDB.aggReplace(db(k),mapping,aggBy,replaceWith,o.v).unique().rename(aggBy)
	
	def aggDB_mapping(db, k, mapping, aggBy, replaceWith):
		o, d1, d2 = partitionDoms(db[k], mapping)
		return AggDB.aggReplace(db(k),mapping,aggBy,replaceWith,o.v).unique().set_names(aggBy,level=replaceWith)
	
	def aggVarSum(var, mapping, aggBy, replaceWith):
		o, d1, d2 = partitionDoms(var, mapping)
		return AggDB.aggReplace(var,mapping,aggBy,replaceWith,o.v).rename_axis(index={replaceWith: aggBy}).groupby(var.index.names).sum().rename(var.name)
	
	def aggVarMean(var, mapping, aggBy, replaceWith):
		o, d1, d2 = partitionDoms(var, mapping)
		return AggDB.aggReplace(var,mapping,aggBy,replaceWith,o.v).rename_axis(index={replaceWith: aggBy}).groupby(var.index.names).mean().rename(var.name)
	
	def aggVarSplitDistr(var,mapping,aggBy,replaceWith,weights=None):
		""" Can be used in one-to-many mappings to split up data with the key 'weights' """
		return (var*weights).dropna().droplevel(aggBy).rename_axis(index={replaceWith: aggBy}).reorder_levels(var.index.names).rename(var.name)
	
	def aggVarWeightedSum(var,mapping,aggBy,replaceWith,weights=None):
		return AggDB.aggVarSum((var*weights).dropna().droplevel(replaceWith).reorder_levels(var.index.names),mapping,aggBy,replaceWith).rename(var.name)
	
	def aggVarWeightedSum_gb(var,mapping,aggBy,replaceWith,weights=None,sumOver=None):
		return AggDB.aggVarWeightedSum(var,weights,mapping,aggBy,replaceWith).groupby([x for x in var.index.names if x not in sumOver]).sum()
	
	def aggVarLambda(var, mapping, aggBy, replaceWith, lambda_=None):
		o, d1, d2 = partitionDoms(var, mapping)
		return AggDB.aggReplace(var,mapping,aggBy,replaceWith,o.v).rename_axis(index={replaceWith: aggBy}).groupby(var.index.names).sum(lambda_).rename(var.name)
		
	def aggReplace(s,mapping,aggBy,replaceWith,overlap):
		return adjMultiIndex.applyMult(s,mapping.droplevel([v for v in mapping.names if v not in [replaceWith]+overlap])).droplevel(aggBy)