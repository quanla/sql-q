package qj.tool.sql;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import qj.tool.sql.Template.Field1;
import qj.util.Cols;
import qj.util.NameCaseUtil;
import qj.util.ReflectUtil;
import qj.util.StringUtil;
import qj.util.funct.F1;
import qj.util.funct.F2;
import qj.util.funct.P1;

import com.google.gson.Gson;

public class Builder<M> {

	private Class<M> clazz;
	private List<String> idFields = Arrays.asList("id");
	private String tableName;
	boolean autoIncrement = true;

	public Builder(Class<M> clazz) {
		this.clazz = clazz;
		tableName = clazz.getSimpleName().toLowerCase();
	}

	public Builder<M> id(String idField) {
		idFields = Arrays.asList(idField);
		return this;
	}

	HashSet<String> dontStore = new HashSet<String>();
	public Template<M> build() {
		Template<M> template = new Template<M>(clazz);
		template.idFields = Cols.yield(idFields, (fName) -> field1(ReflectUtil.getField(fName, clazz)));
		template.dataFields = new LinkedList<>();
		template.tableName = tableName;
		template.autoIncrement = autoIncrement;
		eachField(clazz, (f) -> {
			if (dontStore.contains(f.getName())) {
				return;
			}
			template.dataFields.add(field1(f));
		});
		return template;
	}
	
	public Builder<M> noId() {
		idFields = Collections.emptyList();
		return this;
	}
	public Builder<M> tableName(String tableName) {
		this.tableName = tableName;
		return this;
	}

	public Field1<M> field1(Field field) {
		Field1<M> raw = field1_raw(field);
		F1<Field1<M>, Field1<M>> decor = fieldDecors.get(field.getName());
		if (decor != null) {
			return decor.e(raw);
		}
		return raw;
	}
	
	public static <M> Field1<M> field1_raw(Field field) {
		Field1<M> field1 = new Field1<M>() {
			@Override
			void setValue(Object val, M m) {
				if (boolean.class.equals(field.getType())) {
					if (val == null) {
						val = Boolean.FALSE;
					}
				}
				ReflectUtil.setFieldValue(val, field, m);
			}
			@Override
			Object getValue(M m) {
				return ReflectUtil.getFieldValue(field, m);
			}
		};
		field1.type = field.getType();
		field1.sqlName = NameCaseUtil.camelToHyphen(field.getName());
		field1.psSetter = SQLUtil.setter(field.getType());
		field1.rsGet = rsGet(field.getType());
		return field1;
	}

	Map<String,F1<Field1<M>,Field1<M>>> fieldDecors = new HashMap<>();
	public Builder<M> embededList(String fieldName, Class<?> elemCLass) {
		fieldDecors.put(fieldName, new F1<Field1<M>, Field1<M>>() {
			@Override
			public Field1<M> e(Field1<M> f1) {
				Field1<M> newField1 = new Field1<M>() {
					@Override
					void setValue(Object val, M m) {
						if (val == null || "null".equals(val)) {
							f1.setValue(null, m);
							return;
						}
						Object o = new Gson().fromJson(((String)val), ReflectUtil.forName("[L" + elemCLass.getName() + ";", elemCLass.getClassLoader()));
						f1.setValue(ReflectUtil.invokeMethod("asList", new Object[] {o}, Arrays.class), m);
					}

					@Override
					Object getValue(M m) {
						Object val = f1.getValue(m);
						return new Gson().toJson(val);
					}
				};
				newField1.psSetter = SQLUtil.setter(String.class);
				newField1.rsGet = rsGet(String.class);
				newField1.sqlName = f1.sqlName;
				return newField1;
			}
		});
		return this;
	}
	public Builder<M> embeded(String fieldName) {
		fieldDecors.put(fieldName, (f1) -> {
			Field1<M> newField1 = new Field1<M>() {
				@Override
				void setValue(Object val, M m) {
					if (val == null) {
						f1.setValue(null, m);
						return;
					}
					Object o = new Gson().fromJson(((String)val), f1.type);
					f1.setValue(o, m);
				}

				@Override
				Object getValue(M m) {
					Object val = f1.getValue(m);
					return new Gson().toJson(val);
				}
			};
			newField1.psSetter = SQLUtil.setter(String.class);
			newField1.rsGet = rsGet(String.class);
			newField1.sqlName = f1.sqlName;
			return newField1;
		});
		return this;
	}
	public Builder<M> embededMapBD(String fieldName) {
		fieldDecors.put(fieldName, (f1) -> {
			Field1<M> newField1 = new Field1<M>() {
				@Override
				void setValue(Object val, M m) {
					if (val == null) {
						f1.setValue(null, m);
						return;
					}
					Map o = new Gson().fromJson(((String)val), Map.class);
					for (Object oe : o.entrySet()) {
						Entry e = (Entry)oe;
						o.put(e.getKey(), BigDecimal.valueOf((Double)e.getValue()));
					}
					f1.setValue(o, m);
				}

				@Override
				Object getValue(M m) {
					Object val = f1.getValue(m);
					return new Gson().toJson(val);
				}
			};
			newField1.psSetter = SQLUtil.setter(String.class);
			newField1.rsGet = rsGet(String.class);
			newField1.sqlName = f1.sqlName;
			return newField1;
		});
		return this;
	}

	public Builder<M> dontStore(String fieldName) {
		dontStore.add(fieldName);
		return this;
	}

	private void eachField(Class<?> clazz, P1<Field> p1) {
		for (final Field field : clazz.getDeclaredFields()) {
			int modifiers = field.getModifiers();
			if ((modifiers & (Modifier.STATIC | Modifier.FINAL | Modifier.TRANSIENT)) > 0
					|| (modifiers & Modifier.PUBLIC) == 0
					) {
				continue;
			}
			
			if (idFields.contains(field.getName())) {
				continue;
			}
			p1.e(field);
		}
		if (!clazz.equals(Object.class)) {
			eachField(clazz.getSuperclass(), p1);
		}
	}

	private static F2<ResultSet,Integer,Object> rsGet(Class<?> type) {
		if (type.equals(Date.class)) {
			return (rs, index) -> {
				try {
					return rs.getTimestamp(index);
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			};
		}
		
		Method methodWasNull = ReflectUtil.getMethod("wasNull", ResultSet.class);
		Method methodGet = ReflectUtil.getMethod(rsGetMethodName(type), new Class[] {int.class}, ResultSet.class);
		return (rs, index) -> {
			Object val = ReflectUtil.invoke(methodGet, rs, new Object[] {index});
			Boolean wasNull = ReflectUtil.invoke(methodWasNull, rs);
			
			return wasNull ? null : val;
		};
	}

	private static String rsGetMethodName(Class<?> type) {
		String simpleName = type.getSimpleName();
		if (simpleName.equals("Integer")) {
			simpleName = "Int";
		}
		return "get" + StringUtil.upperCaseFirstChar(simpleName);
	}

	public <MT> Builder<M> fieldConvert(String fieldName, FieldConverter<MT> converter) {
		fieldDecors.put(fieldName, (f1) -> {
			Field1<M> newField1 = new Field1<M>() {
				@Override
				void setValue(Object val, M m) {
					if (val == null) {
						f1.setValue(null, m);
						return;
					}
					f1.setValue(converter.fromDB(val),m);
				}

				@Override
				Object getValue(M m) {
					MT val = (MT) f1.getValue(m);
					return converter.toDB(val);
				}
			};
			newField1.psSetter = SQLUtil.setter(converter.dbType());
			newField1.rsGet = rsGet(converter.dbType());
			newField1.sqlName = f1.sqlName;
			return newField1;
		});
		
		return this;
	}
	public static interface FieldConverter<MT> {

		MT fromDB(Object val);
		Object toDB(MT val);

		Class<?> dbType();
		
	}
	public Builder<M> autoIncrement(boolean b) {
		autoIncrement = b;
		return this;
	}

}