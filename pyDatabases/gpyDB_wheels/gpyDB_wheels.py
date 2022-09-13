from pyDatabases.gpyDB.gpyDB import *
import openpyxl,io

# Content:
# 1. read: Small class of methods to load database from excel data.
# 2. robust: Robust methods to add/merge symbols and databases. 
# 3. adj: A class that adjusts the class 'adj' from _mixedTools.py.
# 4. aggregateDB: A class that aggregates GpyDB databases according to some mappings.

def sunion_empty(ls):
	""" return empty set if the list of sets (ls) is empty"""
	try:
		return set.union(*ls)
	except TypeError:
		return set()

def overlaps(s1,s2):
	doms1,doms2 = getDomains(s1), getDomains(s2)
	return [x for x in doms1 if x in doms2], [x for x in doms1 if x not in doms2], [x for x in doms2 if x not in doms1]

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

class aggregateDB:
	# Main methods:
	# 1. updateSetValues: Update name of set elements in database using a mapping (dictionary)
	# 2. renameSet: Update name of a set in database.
	# 3. read_sets: Add sets to the database by reading established variables/parameters/mappings.
	# 4. update_sets: For existing database clean up set definitions and use 'read_sets' method to redefine sets.
	# 5. subset_db: Subset all symbols in database. 
	# 6. Aggregate database according to mapping. 
	def __init__(self,db):
		self.db = db

	def updateSetValues(self,set_,ns):
		""" Update set values for 'set_' using the namespace 'ns' """
		full_map = {k:k if k not in ns else ns[k] for k in self.db[set_]}
		for k,v in self.db.vardom(set_,types=['set','subset','mapping','parameter','variable']).items():
			[self.updateSetValue_Symbol(k,s,full_map) for s in v];
	
	def updateSetValue_Symbol(self,set_,s,ns):
		if not self.db.get(s).empty:
			if isinstance(self.db.get(s),pd.MultiIndex):
				self.db[s].vals = self.db.get(s).set_levels(self.db.get(s).levels[self.db[s].domains.index(set_)].map(ns),level=set_)
			elif isinstance(self.db.get(s),pd.Index):
				self.db[s].vals = self.db[s].index.map(ns).unique()
			elif isinstance(self.db[s].index,pd.MultiIndex):
				self.db.get(s).index = self.db[s].index.set_levels(self.db[s].index.levels[self.db[s].domains.index(set_)].map(ns),level=set_)
			elif isinstance(self.db[s].index,pd.Index):
				self.db.get(s).index = self.db[s].index.map(ns).unique()
	
	# ----------------------- 2. Rename set names ------------------------- #
	def renameSets(self, ns):
		""" 'ns' is a dictionary with key = original set, value = new set name. This does not alter aliases (unless they are included in 'ns') """
		[self.renameSet(self.db,k,v) for k,v in ns.items()];
	
	def renameSet(self,k,v):
		if k in self.db.symbols:
			self.db[v] = self.db.get(k).rename(v)
			self.db.series.__delitem__(k)
		[self.db.get(vi).__setattr__('index',self.db[vi].index.rename({k:v})) for vi in self.db.vardom(k,types=('variable','parameter'))[k]];
		[self.db.__setitem__(vi, self.db.get(vi).rename({k:v})) for vi in self.db.vardom(k,types=['mapping'])[k]];

	# ----------------------- 3-4. Read sets/update sets from database  ------------------------- #
	def read_sets(self, types=None, ignore_alias=False):
		""" read and define set elements from all symbols of type 'types'. """
		if ignore_alias:
			[add_or_merge_vals(self.db, symbol.index.get_level_values(set_).unique()) for symbol in self.db.getTypes(noneInit(types,['variable','parameter'])).values() for set_ in (set(symbol.domains)-self.db.alias_notin_db)];
		else:
			[add_or_merge_vals(self.db, symbol.index.get_level_values(set_).unique()) for symbol in self.db.getTypes(noneInit(types,['variable','parameter'])).values() for set_ in set(symbol.domains)];
	
	def update_sets(self, types = None, clean=True, ignore_alias=False, clean_alias = False):
		if clean:
			self.clean_sets()
		self.read_sets(types = types, ignore_alias = ignore_alias)
		if clean_alias:
			self.clean_aliases(types)
		self.read_aliased_sets(ignore_alias)
		if clean:
			self.update_subsets_from_sets()
			self.update_maps_from_sets()

	def clean_sets(self):
		""" create empty indices for all sets  """
		[self.db.__setitem__(set_, pd.Index([], name = set_)) for set_ in set(db.getTypes(['set']))-set(['alias_set','alias_map2'])];

	def clean_aliases(self,types):
		""" Remove aliases that are not used in variables/parameters """
		self.db.series['alias_'] = pd.MultiIndex.from_tuples(self.active_aliases(types), names = ['alias_set','alias_map2'])
		self.db.update_alias()

	def active_aliases(self,types):
		""" Return list of tuples with alias_ that are used in the model variables / mappings"""
		return [(k,v) for k in self.db.get('alias_set') for v in [x for x in self.db.alias_dict[k] if len(self.db.vardom(k,types=types)[x])>0]]
	
	def read_aliased_sets(self,ignore_alias):
		""" Read in all elements for aliased sets. If ignore alias"""
		for set_i in self.db.alias_dict:
			all_elements = sunion_empty([set(db.get(set_ij)) for set_ij in self.db.alias_dict0[set_i] if set_ij in self.db.getTypes(['set'])])
			if ignore_alias:
				[self.db.__setitem__(set_ij, pd.Index(all_elements,name=set_ij)) for set_ij in self.db.alias_dict0[set_i] if set_ij in self.db.getTypes(['set'])];
			else:
				[self.db.__setitem__(set_ij, pd.Index(all_elements,name=set_ij)) for set_ij in self.db.alias_dict0[set_i]];
	
	def update_subsets_from_sets(self):
		[self.update_subset(ss) for ss in self.db.getTypes(['subset'])];

	def update_subset(self,ss):
		if self.db.alias(self.db.get(ss).name) not in self.db.symbols:
			self.db.__setitem__(ss,pd.Index([],name=self.db.alias(self.db.get(ss).name)))
		else:
			self.db.__setitem__(ss,adj.rctree_pd(s=self.db[ss],c=self.db[self.db.alias(self.db.get(ss).name)]))
	
	def update_maps_from_sets(self):
		[self.update_map(m) for m in self.db.getTypes(['mapping'])];
	
	def update_map(self,m):
		if sum([bool(set(self.db.symbols.keys()).intersection(self.db.alias_all(s))) for s in self.db[m].domains])<len(self.db[m].domains):
			self.db.__setitem__(m, pd.MultiIndex.from_tuples([], names = self.db[m].domains))
		else:
			self.db.__setitem__(m, adj.rctree_pd(s=self.db[m], c = ('and', [self.db[s] for s in self.db[m].domains])))

	# ----------------------- 5. Subset database with index ------------------------- #
	def subset_db(self,index):
		[self.subset_db_valsFromList(index.rename(k), v) for k,v in self.db.vardom(index.name, types = ('set','subset','mapping','variable','parameter')).items()];

	def subset_db_valsFromList(self,index,listOfSymbols):
		[self.db[symbol].__setattr__('vals', adj.rctree_pd(self.db.get(symbol), index)) for symbol in listOfSymbols];

	# ----------------------- 6. Methods for aggregating database ------------------------- #
	def aggDB(self, mapping, aggBy=None, replaceWith=None, checkUnique=True, AggLike = None):
		""" Aggregate symbols in db according to mapping. This does so inplace, i.e. the set aggBy is altered. 
			Note: The aggregation assumes that mapping is 'one-to-many'; if this is not the case, a warning is printed (if checkUnique) """
		aggBy,replaceWith = noneInit(aggBy, mapping.names[0]), noneInit(replaceWith,mapping.names[-1])
		defaultAggLike = {k: ('Sum',{}) for v,l in self.db.vardom(aggBy).items() for k in l}
		AggLike = defaultAggLike if AggLike is None else defaultAggLike | AggLike
		[self.db.__setitem__(k, self.aggDB_set(k, mapping, aggBy, replaceWith)) for k in self.db.vardom(aggBy,types=['set'])];
		[self.db.__setitem__(vi, self.aggDB_subset(vi, mapping.set_names(k,level=aggBy), k, replaceWith, checkUnique)) for k,v in self.db.vardom(aggBy, types=['subset']).items() for vi in v];
		[self.db.__setitem__(vi, self.aggDB_mapping(vi, mapping.set_names(k,level=aggBy), k, replaceWith, checkUnique)) for k,v in self.db.vardom(aggBy, types=['mapping']).items() for vi in v];
		[self.db.__setitem__(vi, getattr(self,AggLike[vi][0])(self.db.get(vi),mapping.set_names(k,level=aggBy),k,replaceWith,checkUnique,**AggLike[vi][1])) for k,v in self.db.vardom(aggBy).items() for vi in v];
		return self.db

	def aggDB_set(self, k, mapping, aggBy, replaceWith):
		return mapping.get_level_values(replaceWith).unique().rename(k)
	
	def aggDB_subset(self, k, mapping, aggBy, replaceWith,checkUnique):
		o,d1,d2 = overlaps(self.db[k],mapping)
		if checkUnique:
			_checkUnique(self.db[k].index,mapping,o,d1,d2,aggBy,replaceWith,k)
		return self.aggReplace(self.db.get(k),mapping,aggBy,replaceWith,o).unique().rename(aggBy)
	
	def aggDB_mapping(self, k, mapping, aggBy, replaceWith, checkUnique):
		o,d1,d2 = overlaps(self.db[k],mapping)
		if checkUnique:
			self._checkUnique(self.db[k].index,mapping,o,d1,d2,aggBy,replaceWith,k)
		return self.aggReplace(self.db.get(k),mapping,aggBy,replaceWith,o).unique().set_names(aggBy,level=replaceWith)
	
	def aggVarSum(self, var, mapping, aggBy, replaceWith,checkUnique):
		o,d1,d2 = overlaps(var,mapping)
		if checkUnique:
			self._checkUnique(var.index,mapping,o,d1,d2,aggBy,replaceWith,var.name)
		return self.aggReplace(var,mapping,aggBy,replaceWith,o).rename_axis(index={replaceWith: aggBy}).groupby(var.index.names).sum().rename(var.name)
	
	def aggVarMean(self, var, mapping, aggBy, replaceWith,checkUnique):
		o,d1,d2 = overlaps(var,mapping)
		if checkUnique:
			self._checkUnique(var.index,mapping,o,d1,d2,aggBy,replaceWith,var.name)
		return self.aggReplace(var,mapping,aggBy,replaceWith,o).rename_axis(index={replaceWith: aggBy}).groupby(var.index.names).mean().rename(var.name)
	
	def aggVarSplitDistr(self,var,mapping,aggBy,replaceWith,checkUnique,weights=None):
		""" Can be used in many-to-one mappings to split up data with the key 'weights' """
		return (var*weights).dropna().droplevel(aggBy).rename_axis(index={replaceWith: aggBy}).reorder_levels(var.index.names).rename(var.name)
	
	def aggVarWeightedSum(self,var,mapping,aggBy,replaceWith,checkUnique,weights=None):
		return self.aggVarSum((var*weights).dropna().droplevel(replaceWith).reorder_levels(var.index.names),mapping,aggBy,replaceWith,checkUnique).rename(var.name)
	
	def aggVarWeightedSum_gb(self,var,mapping,aggBy,replaceWith,checkUnique,weights=None,sumOver=None):
		return self.aggVarWeightedSum(var,weights,mapping,aggBy,replaceWith,checkUnique).groupby([x for x in var.index.names if x not in sumOver]).sum()
	
	def aggVarLambda(self, var, mapping, aggBy, replaceWith,checkUnique, lambda_=None):
		o,d1,d2 = overlaps(var,mapping)
		if checkUnique:
			self._checkUnique(var.index,mapping,o,d1,d2,aggBy,replaceWith,var.name)
		return self.aggReplace(var,mapping,aggBy,replaceWith,o).rename_axis(index={replaceWith: aggBy}).groupby(var.index).sum(lambda_).rename(var.name)
	
	def _checkUnique(self, index1,index2,o,d1,d2,aggBy,replaceWith,name):
		mi1,mi2 = index1.to_frame().droplevel(d1), index2.reorder_levels(o+d2).to_frame().droplevel(d2)[d2]
		if d2:
			if max(rc_pd(mi2,mi1).groupby(mi2.index.names).nunique()[replaceWith])>1:
				print(f"""**** Warning: The symbol {name} used 'many-to-many' or 'many-to-one'-mapping. Aggregation usually assumes 'one-to-many'.""")
	
	def aggReplace(self,s,mapping,aggBy,replaceWith,overlap):
		return adjMultiIndex.applyMult(s,mapping.droplevel([v for v in mapping.names if v not in [replaceWith]+overlap])).droplevel(aggBy)
