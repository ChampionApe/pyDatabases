from pyDatabases.gpyDB.gpyDB import *
import openpyxl,io

# Content:
# 1. read: Small class of methods to load database from excel data.
# 2. robust: Robust methods to add/merge symbols and databases. 
# 3. adj: A class that adjusts the class 'adj' from _mixedTools.py.


### -------- 	1: Read    -------- ###
class read:
	@staticmethod
	def SeriesDB_from_wb(workbook, kwargs, spliton='/'):
		""" 'read' should be a dictionary with keys = method, value = list of sheets to apply this to."""
		wb = simpleLoad(workbook) if isinstance(workbook,str) else workbook
		db = SeriesDB()
		[robust.merge_dbs_GpyDB(db,getattr(read, function)(wb[sheet],spliton=spliton)) for function, sheets in kwargs.items() for sheet in sheets];
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
	def aux_map(sheet,col,spliton):
		pd_temp = sheet[col]
		pd_temp.columns = [x.split(spliton)[1] for x in pd_temp.iloc[0,:]]
		index = pd.MultiIndex.from_frame(pd_temp.dropna().iloc[1:,:])
		index.name = col
		return gpy(index,**{'name':col})
	@staticmethod
	def aux_var(sheet,col,spliton,type_):
		pd_temp = sheet[col].dropna()
		pd_temp.columns = [x.split(spliton)[1] for x in pd_temp.iloc[0,:]]
		if pd_temp.shape[1]==2:
			index = pd.Index(pd_temp.iloc[1:,0])
		else:
			index = pd.MultiIndex.from_frame(pd_temp.iloc[1:,:-1])
		return gpy(pd.Series(pd_temp.iloc[1:,-1].values,index=index,name=col),**{'type':type_})

	@staticmethod
	def sets(sheet, **kwargs):
		""" Return a dictionary with keys = set names and values = Gpy. na entries are removed. 
			The name of each set is defined as the first entry in each column. """
		pd_sheet = pd.DataFrame(sheet.values)
		return {pd_sheet.iloc[0,i]: gpy(pd.Index(pd_sheet.iloc[1:,i].dropna(),name=pd_sheet.iloc[0,i]),**{'name':pd_sheet.iloc[0,i]}) for i in range(pd_sheet.shape[1])}
	@staticmethod
	def subsets(sheet,spliton='/'):
		pd_sheet = pd.DataFrame(sheet.values)
		return {pd_sheet.iloc[0,i].split(spliton)[0]: gpy(pd.Index(pd_sheet.iloc[1:,i].dropna(),name=pd_sheet.iloc[0,i].split(spliton)[1]),**{'name': pd_sheet.iloc[0,i].split(spliton)[0]}) for i in range(pd_sheet.shape[1])}
	@staticmethod
	def maps(sheet,spliton='/'):
		pd_sheet = pd.DataFrame(sheet.values)
		pd_sheet.columns = [x.split(spliton)[0] for x in pd_sheet.iloc[0,:]]
		return {col: read.aux_map(pd_sheet,col,spliton) for col in set(pd_sheet.columns)}

	def variables(sheet,spliton='/',type_='variable'):
		pd_sheet = pd.DataFrame(sheet.values)
		pd_sheet.columns = [x.split(spliton)[0] for x in pd_sheet.iloc[0,:]]
		return {col: read.aux_var(pd_sheet,col,spliton,type_) for col in set(pd_sheet.columns)}
	
	def parameters(sheet,spliton='/'):
		return read.variables(sheet,spliton=spliton,type_='parameter')
	
	def scalar_variables(sheet,type_='variable',**kwargs):
		pd_sheet = pd.DataFrame(sheet.values)
		return {pd_sheet.iloc[i,0]: gpy(pd_sheet.iloc[i,1],**{'name':pd_sheet.iloc[i,0],'type':type_}) for i in range(pd_sheet.shape[0])}
	
	def scalar_parameters(sheet,**kwargs):
		return read.scalar_variables(sheet,type_='parameter')
	
	def variable2D(sheet,spliton='/',**kwargs):
		""" Read in 2d variable arranged in matrix; Note, only reads 1 variable per sheet."""
		pd_sheet = pd.DataFrame(sheet.values)
		domains = pd_sheet.iloc[0,0].split(spliton)
		var = pd.DataFrame(pd_sheet.iloc[1:,1:].values, index = pd.Index(pd_sheet.iloc[1:,0],name=domains[1]), columns = pd.Index(pd_sheet.iloc[0,1:], name = domains[2])).stack()
		var.name = domains[0]
		return {domains[0]: gpy(var,**kwargs)}
	

### -------- 	2: Merge databases    -------- ###
class robust:
	@staticmethod
	def robust_merge_dbs(db1,db2,priority=None):
		""" merge db2 into db1; priority = 'first' uses db1 if there is overlap. 'second' uses db2. This is much slower if priority = 'first'. """
		if isinstance(db1,(GpyDB,SeriesDB,dict)):
			if isinstance(db2,(GpyDB,SeriesDB,dict)):
				robust.merge_dbs_GpyDB(db1,db2,priority=priority)
			elif isinstance(db2,gams.GamsDatabase):
				robust.merge_dbs_GpyDB_gams(db1,db2,robust.get_g2np(db2),priority=priority)
		elif isinstance(db1,gams.GamsDatabase):
			if isinstance(db2,(GpyDB,SeriesDB,dict)):
				robust.merge_dbs_gams_GpyDB(db1,db2,robust.get_g2np(db1),priority=priority)
			elif isinstance(db2,gams.GamsDatabase):
				robust.merge_dbs_gams(db1,db2,robust.get_g2np(db1),priority=priority)
	@staticmethod
	def get_g2np(db):
		if isinstance(db,gams.GamsDatabase):
			return gams2numpy.Gams2Numpy(db.workspace.system_directory)
		elif isinstance(db,GpyDB):
			return db.g2np
		else:
			raise TypeError(f"db of type {type(db)} cannot access g2np.")
	@staticmethod
	def iters_db_py(db):
		return db if isinstance(db,(GpyDB,SeriesDB)) else db.values()
	@staticmethod
	def merge_dbs_GpyDB(db1,db2,priority=None):
		"""" merge db2 into db1. """
		if priority in ['second',None]:
			[GpyDBs_AOM_Second(db1,symbol) for symbol in robust.iters_db_py(db2)];
		elif priority== 'first':
			[GpyDBs_AOM_First(db1,symbol) for symbol in robust.iters_db_py(db2)];
	@staticmethod
	def merge_dbs_GpyDB_gams(db_py,db_gms,g2np,priority=None):
		""" Merge db_gms into db_py. """
		if priority in ['second',None]:
			[GpyDBs_AOM_Second(db_py,symbol) for symbol in dict_from_GamsDatabase(db_gms,g2np).values()];
		elif priority == 'first':
			[GpyDBs_AOM_First(db_py,symbol) for symbol in dict_from_GamsDatabase(db_gms,g2np).values()];
	@staticmethod
	def merge_dbs_gams_GpyDB(db_gms,db_py,g2np,priority=None):
		""" merge db_py into db_gms."""
		if priority in ['second',None]:
			[gpy2db_gams_AOM(s,db_gms,g2np,merge=True) for s in robust.iters_db_py(db_py)];
		elif priority == 'first':
			if isinstance(db_py,GpyDB):
				d = db_py.series.database.copy()
			elif isinstance(db_py,SeriesDB):
				d = db_py.symbols.copy()
			elif type(db_py) is dict:
				d = db_py.copy()
			robust.merge_dbs_GpyDB_gams(d, db_gms, g2np,priority='second') # merge db_gms into dictionary of gpy symbols.
			robust.merge_dbs_gams_GpyDB(db_gms, d, g2np,priority='second') # merge gpy symbols into gams.
	@staticmethod
	def merge_dbs_gams(db1,db2,g2np,priority=None):
		""" Merge db2 into db1. """
		if priority in ['second',None]:
			[gpy2db_gams_AOM(s,db1,g2np,merge=True) for s in dict_from_GamsDatabase(db2,g2np).values()];
		elif priority=='first':
			d = dict_from_GamsDatabase(db2,g2np) # copy of db2.
			robust.merge_dbs_GpyDB_gams(d,db1,g2np) # merge into d with priority to db1.
			robust.merge_dbs_gams_GpyDB(db1,d,g2np,priority='second') 

	@staticmethod	
	def robust_gpy(symbol,db=None,g2np=None,**kwargs):
		if isinstance(symbol,admissable_gpy_types):
			return gpy(symbol,**kwargs)
		elif isinstance(symbol,admissable_gamsTypes):
			return gpy(gpydict_from_GamsSymbol(db, g2np, symbol))
		else:
			try:
				return gpy(gpydict_from_GmdSymbol(db,g2np,symbol))
			except:
				raise TypeError(f"Tried to initiate gpy symbol from gams.Database._gmd. Check consistency of types: {type(symbol),type(db)}.")
	@staticmethod
	def robust_add(db,symbol,db_from=None,g2np=None,merge=False,**kwargs):
		""" Symbol ∈ {gams.database._GamsSymbol, pandas-like symbol, gpy}"""
		s = robust.robust_gpy(symbol,db=db_from, g2np = g2np, **kwargs)
		if isinstance(db,(dict, GpyDB, SeriesDB)):
			db[s.name] = s
		elif isinstance(db, gams.GamsDatabase):
			robust.gpy2db_gams(s,db[s.name],db,g2np,merge=merge)
		else:
			raise TypeError("Check type(db).")
	@staticmethod
	def robust_add_or_merge(db,symbol,db_from=None,g2np=None,merge=True,**kwargs):
		""" If 'symbol' exists in db merge with priority to new values in 'symbol'. """
		s = robust.robust_gpy(symbol,db=db_from, g2np = g2np, **kwargs)
		if isinstance(db,(dict, GpyDB,SeriesDB)):
			if s.name in symbols_db(db):
				db[s.name].vals = merge_gpy_vals(s.vals, db[s.name].vals)
			else:
				db[s.name] = s
		elif isinstance(db, gams.GamsDatabase):
			gpy2db_gams_AOM(s,db,g2np,merge=merge)
		else:
			raise TypeError("Check type(db).")
	
class adj(adj):
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
		elif isinstance(symbol,gpy):
			return adj.rc_AdjPd(symbol.vals, alias = alias, lag = lag)
		elif isinstance(symbol, (int,float,np.generic)):
			return symbol
		else:
			raise TypeError(f"Input was type {type(symbol)}")
	@staticmethod
	def rc_pd(s=None,c=None,alias=None,lag=None, pm = True, **kwargs):
		if isinstance(s,(int,float,np.generic)):
			return s
		elif isinstance(s, gpy) and (s.type in ('scalar_variable','scalar_parameter')):
			return s.vals
		else:
			return adj.rctree_pd(s=s, c = c, alias = alias, lag = lag, pm = pm, **kwargs)
	@staticmethod
	def rc_pdInd(s=None,c=None,alias=None,lag=None,pm=True,**kwargs):
		if isinstance(s,(int,float,np.generic)) or (isinstance(s,gpy) and (s.type in ('scalar_variable','scalar_parameter'))):
			return None
		else:
			return adj.rctree_pdInd(s=s,c=c,alias=alias,lag=lag,pm=pm,**kwargs)