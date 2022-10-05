from pyDatabases._mixedTools import *
import os, gams, pickle, gams2numpy, gamstransfer, gmdcc

#global settings
default_attributes_variables = {k:i for k,i in zip(['level','marginal','lower','upper','scale'],range(-5,0))}
default_attributes_parameters= {k:i for k,i in zip(['value'],range(-1,0))}
admissable_py_types = (pd.Series,pd.Index,int,float,str,np.generic)
admissable_gamsTypes = (gams.GamsSet, gams.GamsParameter, gams.GamsVariable)


# Content:
# 0: Auxiliary functions used throughout
# 1: Methods used to identify the type of a symbol.
# 2: Methods used to write gams symbols to pandas representation (gpy_symbols).
#	 Includes methods for gams._GamsSymbol symbols and symbols from gams._gmd databases (Swig objects). 
# 3: From pandas/python symbol to gamstransfer.Container
# 4: From pandas/python symbol to database.
# 5. gpy: Class of user-defined version of symbols.
# 6: A few methods to merge symbols

### -------- 	0: Auxiliary functions    -------- ###
def symbols_db(db):
	""" return dictionary like version of database""" 
	return db if type(db) is dict else {symbol.name: symbol for symbol in db}

def tryint(x):
	try:
		return x.astype(int)
	except ValueError:
		return x

### -------- 	1: Identify types    -------- ###
def type_pandas_(symbol,name=None,type=None,**kwargs):
	if isinstance(symbol, pd.Series):
		return type if type else 'variable'
	elif isinstance(symbol,pd.MultiIndex):
		return 'mapping'
	elif isinstance(symbol,pd.Index):
		return set_or_subset(symbol.name,name)
	elif isinstance(symbol,(int,float,str,np.generic)):
		return 'scalar_parameter' if type in ('parameter','scalar_parameter') else 'scalar_variable'

def set_or_subset(s_name,name):
	return 'set' if s_name == name else 'subset'

def type_gamsSymbol_(symbol):
	if isinstance(symbol,gams.GamsVariable):
		return type_GamsVariable(symbol)
	elif isinstance(symbol,gams.GamsParameter):
		return type_GamsParameter(symbol)
	elif isinstance(symbol,gams.GamsSet):
		return type_GamsSet(symbol)
def type_GamsVariable(domains_as_strings):
	return 'scalar_variable' if not domains_as_strings else 'variable'
def type_GamsParameter(domains_as_strings):
	return 'scalar_parameter' if not domains_as_strings else 'parameter' 
def type_GamsSet(name, domains_as_strings):
	if len(domains_as_strings)==1:
		if domains_as_strings in [['*'], [name]]:
			return 'set'
		else:
			return 'subset'
	elif name!='SameAs':
		return 'mapping'

### -------- 	2: GAMS to pandas/gpy    -------- ###
# 2.1: From numpy like data to gpy-arranged dictionaries:
def np_from_gams(name,db_gams,g2np):
	return g2np.gmdReadSymbolStr(db_gams,name)

def df_from_variable(np_data,symbol_name,domains_as_strings,attributes=['level','marginal','lower','upper','scale']):
	return pd.DataFrame(np_data[:,[default_attributes_variables[k] for k in attributes]], index = index_from_np(np_data,symbol_name,domains_as_strings), columns = attributes)

def df_from_parameter(np_data,symbol_name,domains_as_strings,attributes=['value']):
	return pd.DataFrame(np_data[:,[default_attributes_parameters[k] for k in attributes]], index = index_from_np(np_data,symbol_name,domains_as_strings), columns = attributes)

def index_from_np(np_data,symbol_name,domains_as_strings):
	if len(domains_as_strings) > 1:
		return pd.MultiIndex.from_arrays([tryint(column) for column in np_data.T[0:len(domains_as_strings)]], names = index_names_from_np(symbol_name,domains_as_strings))
	elif len(domains_as_strings) == 1:
		return pd.Index(tryint(np_data[:,0]),name=index_names_from_np(symbol_name,domains_as_strings)[0])
	else:
		return None

def index_names_from_np(symbol_name,domains_as_strings):
	return [symbol_name] if domains_as_strings == ["*"] else domains_as_strings

def adjust_scalar(domains_as_strings,vals):
	return vals if domains_as_strings else vals[0]

def gpydict_from_Set(db_gams, g2np, name, domains_as_strings,type_,text=None):
	return {'vals': index_from_np(np_from_gams(name, db_gams, g2np), name, domains_as_strings), 'name': name, 'text': "" if text is None else text, 'type': type_}

def gpydict_from_Variable(db_gams, g2np, name, domains_as_strings,type_, text=None):
	return {'vals': adjust_scalar(domains_as_strings, df_from_variable(np_from_gams(name,db_gams,g2np), name, domains_as_strings,**{'attributes': ['level']})['level']), 'name': name, 'text': "" if text is None else text, 'type': type_}

def gpydict_from_Parameter(db_gams, g2np, name, domains_as_strings,type_, text=None):
	return {'vals': adjust_scalar(domains_as_strings, df_from_parameter(np_from_gams(name,db_gams,g2np), name, domains_as_strings)['value']), 'name': name, 'text': "" if text is None else text, 'type': type_}

# 2.2: From GamsDatabase._gmd symbols to gpy_symbol dictionaries:
def gpydict_from_GmdSymbol(db_gmd,g2np,symbol):
	if TypeFromGmdSymbol_NotAlias(db_gmd, symbol) == 'set':
		return gpydict_from_GmdSet(db_gmd,g2np,symbol)
	elif TypeFromGmdSymbol_NotAlias(db_gmd, symbol) == 'variable':
		return gpydict_from_GmdVariable(db_gmd, g2np, symbol)
	elif TypeFromGmdSymbol_NotAlias(db_gmd, symbol) == 'parameter':
		return gpydict_from_GmdParameter(db_gmd, g2np, symbol)

def gpydict_from_GmdSet(db_gmd,g2np,symbol):
	attrs = AttrFromGmd(db_gmd,symbol)
	return gpydict_from_Set(db_gmd, g2np, attrs['name'], attrs['domains_as_strings'], type_GamsSet(attrs['name'],attrs['domains_as_strings']), text=attrs['text'])

def gpydict_from_GmdVariable(db_gmd,g2np,symbol):
	attrs = AttrFromGmd(db_gmd,symbol)
	return gpydict_from_Variable(db_gmd, g2np, attrs['name'], attrs['domains_as_strings'], type_GamsVariable(attrs['domains_as_strings']), text=attrs['text'])

def gpydict_from_GmdParameter(db_gmd,g2np,symbol):
	attrs = AttrFromGmd(db_gmd,symbol)
	return gpydict_from_Parameter(db_gmd, g2np, attrs['name'], attrs['domains_as_strings'], type_GamsParameter(attrs['domains_as_strings']), text=attrs['text'])

def gpydict_Alias_from_Gmd(db_gmd, symbols, rc):
	return {'vals': AliasFromGmd(db_gmd,symbols,rc), 'name': 'alias_', 'type': 'mapping', 'text': 'Aliased sets'}

def AttrFromGmd(db_gmd,symbol):
	return {'name': NameFromGmd(db_gmd, symbol), 'domains_as_strings': gmdcc.gmdGetDomain(db_gmd,symbol, gmdcc.gmdSymbolInfo(db_gmd,symbol,gmdcc.GMD_DIM)[1])[-1],
	 'text': gmdcc.gmdSymbolInfo(db_gmd, symbol, gmdcc.GMD_EXPLTEXT)[-1], 'type': TypeFromGmdSymbol_NotAlias(db_gmd, symbol)}

def NameFromGmd(db_gmd, symbol):
	return gmdcc.gmdSymbolInfo(db_gmd, symbol, gmdcc.GMD_NAME)[-1]

def TypeFromGmdSymbol_NotAlias(db_gmd, symbol):
	return gamstransfer._Symbol._sym_object_map[(gmdcc.gmdSymbolType(db_gmd, symbol)[-1]), gmdcc.gmdSymbolInfo(db_gmd, symbol, gmdcc.GMD_USERINFO)[1]]

def IsGmdSymbolAlias(db_gmd,symbol):
	return gmdcc.gmdSymbolType(db_gmd, symbol)[-1]==gmdcc.GMS_DT_ALIAS

def IsGmdSymbolNotAliasEq(db_gmd,symbol):
	return gmdcc.gmdSymbolType(db_gmd,symbol)[-1] not in (gmdcc.GMS_DT_ALIAS,gmdcc.GMS_DT_EQU)

def AliasFromGmd(db_gmd,symbols,rc):
	return pd.MultiIndex.from_tuples([AliasTupleFromGmd(db_gmd,s,rc) for s in symbols if IsGmdSymbolAlias(db_gmd, s)],names=['alias_set','alias_map2'])

def AliasTupleFromGmd(db_gmd,symbol,rc):
	return (GetParentFromGmd(db_gmd, gmdcc.gmdSymbolInfo(db_gmd, symbol, gmdcc.GMD_USERINFO)[1],rc), NameFromGmd(db_gmd, symbol))

def GetParentFromGmd(db_gmd,subtype,rc):
	return gmdcc.gmdSymbolInfo(db_gmd, gmdcc.gmdGetSymbolByNumberPy(db_gmd,subtype-1,rc), gmdcc.GMD_NAME)[-1]

# 2.3: From gams._GamsSymbol to gpy_symbol dictionaries:
def gpydict_from_GamsSymbol(db_gams, g2np, symbol):
	if isinstance(symbol, gams.GamsSet):
		return gpydict_from_GamsSet(db_gams,g2np,symbol)
	elif isinstance(symbol, gams.GamsVariable):
		return gpydict_from_GamsVariable(db_gams,g2np,symbol)
	elif isinstance(symbol, gams.GamsParameter):
		return gpydict_from_GamsParameter(db_gams,g2np,symbol)

def gpydict_from_GamsSet(db_gams, g2np, symbol):
	return gpydict_from_Set(db_gams, g2np, symbol.name, symbol.domains_as_strings, type_GamsSet(symbol.name, symbol.domains_as_strings), symbol.text)

def gpydict_from_GamsVariable(db_gams, g2np, symbol):
	return gpydict_from_Variable(db_gams, g2np, symbol.name, symbol.domains_as_strings, type_GamsVariable(symbol.domains_as_strings), symbol.text)

def gpydict_from_GamsParameter(db_gams, g2np, symbol):
	return gpydict_from_Parameter(db_gams, g2np, symbol.name, symbol.domains_as_strings, type_GamsParameter(symbol.domains_as_strings), symbol.text)

### -------- 	3: Pandas/python to gams container    -------- ###
def gamstransfer_from_py_(symbol,container):
	""" symbol = gpy, container = gamstransfer.Container """
	if symbol.type == 'set':
		gamstransferSet_from_py(symbol.index, container, description=symbol.text)
	elif symbol.type == 'subset':
		gamstransferSubset_from_py(symbol.index, symbol.name, container, description = symbol.text)
	elif symbol.type == 'mapping':
		gamstransferMapping_from_py(symbol.index, symbol.name, container, description = symbol.text)
	elif symbol.type == 'scalar_variable':
		gamstransferScalarVariable_from_py(symbol.vals, symbol.name, container, description = symbol.text)
	elif symbol.type =='variable':
		gamstransferVariable_from_py(symbol.df, symbol.name, container, description=symbol.text)
	elif symbol.type == 'scalar_parameter':
		gamstransferScalarParameter_from_py(symbol.vals, symbol.name, container, description=symbol.text)
	elif symbol.type == 'parameter':
		gamstransferParameter_from_py(symbol.df, symbol.name, container, description=symbol.text)

def gamstransferSet_from_py(index,container,description=None):
	gamstransfer.Set(container, index.name, description="" if description is None else description, records = index.astype(str))

def gamstransferSubset_from_py(index,name,container,description=None):
	gamstransfer.Set(container, name, index.name, description="" if description is None else description, records = index.astype(str))

def gamstransferMapping_from_py(index,name,container,description=None):
	gamstransfer.Set(container, name, index.names, description="" if description is None else description, records = index.to_frame(index=False).astype(str))

def gamstransferScalarVariable_from_py(scalar, name, container, description=None):
	if isinstance(scalar,pd.DataFrame):
		gamstransfer.Variable(container, name, description = "" if description is None else description, records = scalar)
	elif not is_iterable(scalar):
		gamstransfer.Variable(container, name, description = "" if description is None else description, records = pd.DataFrame([scalar], columns = ['level']))

def gamstransferVariable_from_py(df, name, container, description = None):
	gamstransfer.Variable(container, name, domain= df.index.names, description="" if description is None else description, records = df.reset_index().astype({k:str for k in df.index.names}))

def gamstransferScalarParameter_from_py(scalar, name, container, description=None):
	gamstransfer.Parameter(container, name, description="", records = scalar)

def gamstransferParameter_from_py(df, name, container, description= None):
	gamstransfer.Parameter(container, name, domain = df.index.names, description="" if description is None else description, records = df.reset_index().astype({k:str for k in df.index.names}))

### -------- 	4: Pandas/python to database    -------- ###
def gpy2db_gams_AOM(s,db,g2np,merge=True):
	if s.name in gamsdb_symbols(db):
		gpy2db_gams(s,db[s.name],db,g2np,merge=merge)
	else:
		AddSymbol2db_gams(s,db,g2np)

def gamsdb_symbols(db):
	return {s.name: s for s in db}

def gpy2db_gams(s,symbolPtr,db,g2np,merge=False):
	if s.type in ('set','subset','mapping'):
		g2np.gmdFillSymbolStr(db,symbolPtr,set2np(s),merge=merge)
	elif s.type == 'scalar_parameter':
		g2np.gmdFillSymbolStr(db,symbolPtr,pscalar2np(s),merge=merge)
	elif s.type == 'parameter':
		g2np.gmdFillSymbolStr(db,symbolPtr,parameter2np(s),merge=merge)
	elif s.type == 'scalar_variable':
		g2np.gmdFillSymbolStr(db,symbolPtr,scalar2np(s),merge=merge)
	elif s.type == 'variable':
		g2np.gmdFillSymbolStr(db,symbolPtr,variable2np(s),merge=merge)

def AddSymbol2db_gams(s,db,g2np):
	if s.type == 'set':
		g2np.gmdFillSymbolStr(db,db.add_set(s.name,1,s.text),set2np(s),merge=False)
	elif s.type in ('subset','mapping'):
		g2np.gmdFillSymbolStr(db,db.add_set_dc(s.name,s.domains,s.text),set2np(s),merge=False)
	elif s.type == 'scalar_parameter':
		g2np.gmdFillSymbolStr(db,db.add_parameter(s.name,0,s.text),pscalar2np(s),merge=False)
	elif s.type == 'parameter':
		g2np.gmdFillSymbolStr(db,db.add_parameter_dc(s.name,s.domains,s.text),parameter2np(s),merge=False)
	elif s.type == 'scalar_variable':
		g2np.gmdFillSymbolStr(db,db.add_variable(s.name,0,gams.VarType.Free,s.text),scalar2np(s),merge=False)
	elif s.type == 'variable':
		g2np.gmdFillSymbolStr(db,db.add_variable_dc(s.name,gams.VarType.Free,s.domains,s.text),variable2np(s),merge=False)

def AdjIndex(df):
	return df.reset_index().astype({k: str for k in df.index.names})
def set2np(s):
	return s.vals.to_frame(index=False).assign(_rname="").astype(str).to_numpy()
def pscalar2np(s):
	return np.array([[s.vals]])
def parameter2np(s):
	return AdjIndex(s.vals).to_numpy()
def scalar2np(s):
	return np.array([list((gamstransfer.Variable._default_values['free'] | {'level': s.vals}).values())])
def variable2np(s):
	return AdjIndex(s.df.assign(**{k:v for k,v in gamstransfer.Variable._default_values['free'].items() if k!='level'})).to_numpy()


### -------- 	5. gpy: class of symbols    -------- ###
class gpy:
	""" Customized class of symbols used in the PM/GPM database classes. """
	def __init__(self,symbol,**kwargs):
		""" Initialize with gpy, dict, or pandas object. Fast init with suitable dict/gpy. """
		if isinstance(symbol,(gpy,dict)):
			[setattr(self,key,value) for key,value in symbol.items() if key not in kwargs];
			[setattr(self,key,value) for key,value in kwargs.items()];
		elif isinstance(symbol,admissable_py_types):
			self.vals = symbol
			self.name = kwargs['name'] if 'name' in kwargs else symbol.name
			self.type = type_pandas_(symbol, **kwargs)
			self.text = dictInit('text',"",kwargs)

	def __iter__(self):
		return iter(self.vals)

	def __len__(self):
		return len(self.vals)

	def items(self):
		return self.__dict__.items()

	def copy(self):
		obj = type(self).__new__(self.__class__,None)
		obj.__dict__.update(self.__dict__)
		return obj

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
	def df(self):
		""" Only works if self.vals is a pandas series + type is either variable/parameter."""
		return self.vals.to_frame(name='level' if self.type == 'variable' else 'value')


### -------- 	6: Add or merge symbols    -------- ###
def merge_gpy_vals(s1,s2):
	if isinstance(s1,pd.Series):
		return s1.combine_first(s2)
	elif isinstance(s1,pd.Index):
		return s1.union(s2)
	elif type_pandas_(s1) in ['scalar_variable','scalar_parameter']:
		return s1

def GpyDBs_AOM_Second(db,symbol):
	if symbol.name in symbols_db(db):
		db[symbol.name].vals = merge_gpy_vals(symbol.vals, db[symbol.name].vals)
	else:
		db[symbol.name] = symbol
def GpyDBs_AOM_First(db,symbol):
	if symbol.name in symbols_db(db):
		db[symbol.name].vals = merge_gpy_vals(db[symbol.name].vals,symbol.vals)
	else:
		db[symbol.name] = symbol

def add_or_merge_vals(db,symbol,name=None):
	if name is None:
		name = symbol.name
	if name in symbols_db(db):
		db[name].vals = merge_gpy_vals(symbol, db[name].vals)
	else:
		db[name] = symbol
