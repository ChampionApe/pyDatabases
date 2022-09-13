from pyDatabases._mixedTools import *
from pyDatabases.simpleDB.simpleDB import SimpleDB, type_
import openpyxl,io

# 1: Read methods:
class read:
	@staticmethod
	def dbFromWB(workbook, kwargs, spliton='/'):
		""" 'read' should be a dictionary with keys = method, value = list of sheets to apply this to."""
		wb = read.simpleLoad(workbook) if isinstance(workbook,str) else workbook
		db = SimpleDB()
		[db.mergeDbs(getattr(read, function)(wb[sheet],spliton=spliton)) for function,sheets in kwargs.items() for sheet in sheets];
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
		return {col: read.aux_map(pd_sheet,col,spliton) for col in set(pd_sheet.columns)}

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
		return {col: read.aux_var(pd_sheet,col,spliton) for col in set(pd_sheet.columns)}

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

# 2: 
def readSets(db, types = None):
	""" Read sets from database symbols """
	[db.addOrMerge(set_, getIndex(symbol).get_level_values(set_).unique()) for symbol in db.getTypes(noneInit(types,['variable'])).values() for set_ in getIndex(symbol).names];

# 3: Broadcasting-like methods
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

