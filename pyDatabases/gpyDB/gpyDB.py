from ._database import *
admissable_gpy_types = (pd.Series,pd.Index,int,float,str,np.generic,dict,gpy)

# A few useful database functions
def dict_from_GamsDatabase(db_gams,g2np):
	return {symbol.name: gpy(gpydict_from_GamsSymbol(db_gams, g2np, symbol)) for symbol in db_gams if type(symbol) in admissable_gamsTypes}

def dict_from_GmdDatabase(db_gmd,g2np):
	rc = gmdcc.new_intp()
	syms = (gmdcc.gmdGetSymbolByNumberPy(db_gmd, i, rc) for i in range(gmdcc.gmdInfo(db_gmd, gmdcc.GMD_NRSYMBOLSWITHALIAS)[1]))
	return {NameFromGmd(db_gmd, s): gpy(gpydict_from_GmdSymbol(db_gmd,g2np,s)) for s in syms if IsGmdSymbolNotAliasEq(db_gmd, s)} | {'alias_': gpydict_Alias_from_Gmd(db_gmd, syms, rc)}

def gams_from_db_py(db_py,db_gams,g2np,merge=False):
	[gpy2db_gams_AOM(s,db_gams,g2np,merge=merge) for s in db_py];

class SeriesDB:
	""" Basically a wrapper that makes sure that iter works through self.database.values()"""
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

	def get(self, item):
		return self.database[item].vals

	def copy(self):
		obj = type(self).__new__(self.__class__,None)
		obj.__dict__.update({k:v for k,v in self.__dict__.items() if k != 'database'})
		obj.database = {k:gpy(v) for k,v in self.database.items()}
		return obj

class GpyDB:
	def __init__(self,pickle_path=None,ws=None,db=None,alias=None,**kwargs):
		""" Init methods: 	(1) from file path to a pickle (fastest, only uses the kwargs 'ws','alias'), 
							(2) db = GpyDB (fast, only uses the kwarg 'ws','alias'), 
							(3) db = dict (fast, but does not check validity of attributes. only uses 'ws','alias'), 
							(4) db ∈ {None, str, gams.GamsDatabase} (slower, but more robust. uses all kwargs except 'pickle_path'); """ 
		if pickle_path is not None:
			self.init_from_pickle(pickle_path,ws=ws)
		elif isinstance(db,dict):
			self.update_with_ws(ws,db)
		elif isinstance(db,GpyDB):
			self.init_from_GpyGB(db,ws=ws)
		else:
			self.init_ws(ws)
			self.g2np = gams2numpy.Gams2Numpy(self.ws.system_directory)
			self.name = self.versionized_name(dictInit('name','rname',kwargs))
			self.export_settings = {'dropattrs': ['database','ws','g2np'], 'data_folder': dictInit('data_folder',os.getcwd(),kwargs)}
			self.init_dbs(db)
			self.series = SeriesDB(database=dict_from_GamsDatabase(self.database,self.g2np))
		self.update_alias(alias=alias)

	###################################################################################################
	###									0: Pickling/load/export settings 							###
	###################################################################################################
	def versionized_name(self,name):
		""" test if name is available in the current ws; update with an added if it is not """
		return return_version(name,self.ws._gams_databases)

	def init_from_pickle(self,pickle_path,ws=None):
		with open(pickle_path,"rb") as file:
			pickled_data=pickle.load(file)
		self.update_with_ws(ws,pickled_data.__dict__)

	def init_from_GpyGB(self,db,ws=None):
		self.update_with_ws(ws,db.__dict__)

	def update_with_ws(self,ws,dict_):
		if ws is None:
			self.__dict__ = dict_
		else:
			self.__dict__ = {key: value for key,value in dict_.items() if key not in ('ws','database')}
			self.init_ws(ws)
			self.name = self.versionized_name(self.name) # update name of database if it is already used in the current ws.
			self.init_dbs(dict_['database'])

	def init_ws(self,ws):
		if ws is None:
			self.ws = gams.GamsWorkspace()
		elif type(ws) is str:
			self.ws = gams.GamsWorkspace(working_directory=ws)
		elif isinstance(ws,gams.GamsWorkspace):
			self.ws = ws
		self.work_folder = self.ws.working_directory

	def init_dbs(self,db):
		if db is None:
			self.database = self.ws.add_database(database_name=self.name)
		elif type(db) is str:
			self.database = self.ws.add_database_from_gdx(db,database_name=self.name)
		elif isinstance(db,gams.GamsDatabase):
			self.database = self.ws.add_database(source_database=db,database_name=self.name)
		elif isinstance(db,Gpy_DB):
			self.database = self.ws.add_database(source_database=db.database,database_name=self.name)

	def update_alias(self,alias=None):
		alias = pd.MultiIndex.from_tuples([],names=['alias_set','alias_map2']) if alias is None else alias.set_names(['alias_set','alias_map2'])
		if 'alias_' not in self.series.database:
			self.series['alias_'] = alias
		else:
			self.series['alias_'] = self.get('alias_').union(alias)
		self.series['alias_set'] = self.get('alias_').get_level_values('alias_set').unique()
		self.series['alias_map2'] = self.get('alias_').get_level_values('alias_map2').unique()

	def __getstate__(self):
		if 'database' not in self.export_settings['dropattrs']:
			self.database.export(self.export_settings['data_folder']+'\\'+self.name+'.gdx')
		return {key: value for key,value in self.__dict__.items() if key not in (self.export_settings['dropattrs']+['database'])}

	def __setstate__(self,dict_):
		self.__dict__ = dict_
		try:
			self.ws = gams.GamsWorkspace(working_directory=self.work_folder)
		except FileNotFoundError:
			self.ws = gams.GamsWorkspace(working_directory=os.getcwd())
		self.g2np = gams2numpy.Gams2Numpy(self.ws.system_directory)
		if 'database' not in self.export_settings['dropattrs']:
			self.database = self.ws.add_database_from_gdx(self.export_settings['data_folder']+'\\'+self.name+'.gdx')
		else:
			self.database = self.ws.add_database()
		if 'series' in self.export_settings['dropattrs']:
			self.series = SeriesDB(database=dict_from_GamsDatabase(self.database,self.g2np))
	
	def export(self,name=None,repo=None):
		name = self.name if name is None else name
		repo = self.export_settings['data_folder'] if repo is None else repo
		with open(repo+'\\'+name, "wb") as file:
			pickle.dump(self,file)

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

	def get(self,item):
		try:
			return self.series[item].vals
		except KeyError:
			return self.series[self.alias(item)].vals.rename(item)

	@property
	def symbols(self):
		return self.series.database

	def getTypes(self,types):
		return {symbol.name: symbol for symbol in self.series if symbol.type in types}

	def copy(self,dropattrs=None,**kwargs):
		""" return copy of database. Ignore elements in dropattrs."""
		db = GpyDB(**{**self.__dict__,**kwargs})
		if 'series' not in noneInit(dropattrs,['database']) and 'series' not in kwargs.keys():
			db.series = self.series.copy()
		return db

	###################################################################################################
	###									2: Dealing with aliases			 							###
	###################################################################################################

	@property
	def alias_dict(self):
		return {name: self.get('alias_').get_level_values(1)[self.get('alias_').get_level_values(0)==name] for name in self.get('alias_set')}

	@property
	def alias_dict0(self):
		return {key: self.alias_dict[key].insert(0,key) for key in self.alias_dict}

	@property
	def alias_notin_db(self):
		return set(self.get('alias_map2'))-set(self.getTypes(['set']))

	def alias_all(self,x):
		if x in self.get('alias_set').union(self.get('alias_map2')):
			return self.alias_dict0[self.alias(x)]
		else: 
			return [x]

	def alias(self,x,index_=0):
		if x in self.get('alias_set'):
			return self.alias_dict0[x][index_]
		elif x in self.get('alias_map2'):
			key = self.get('alias_').get_level_values(0)[self.get('alias_').get_level_values(1)==x][0]
			return self.alias_dict0[key][index_]
		elif x in self.getTypes(['set','subset','mapping']) and index_==0:
			return x
		else:
			raise TypeError(f"{x} is not aliased")

	def domains_unique(self,x):
		""" Returns list of sets a symbol x is defined over. If x is defined over a set and its alias, only the set is returned. """
		return np.unique([self.alias(name) for name in self[x].index.names]).tolist()

	def vardom(self,set_,types=None):
		""" Returns a dict with keys = set_+aliases, values = list of symbols in 'types' defined over the the relevant set/alias"""
		return {set_i: [k for k,v in self.getTypes(noneInit(types,['variable','parameter'])).items() if set_i in v.domains] for set_i in self.alias_all(set_)}

	def merge_internal(self,merge=True):
		gams_from_db_py(self.series,self.database,self.g2np,merge=merge)


