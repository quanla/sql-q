package qj.tool.sql;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import qj.tool.sql.test.Aa;
import qj.util.Cols;
import qj.util.IOUtil;
import qj.util.NameCaseUtil;
import qj.util.ReflectUtil;
import qj.util.RegexUtil;
import qj.util.StringUtil;
import qj.util.funct.F1;
import qj.util.funct.F2;
import qj.util.funct.P1;
import qj.util.funct.P3;

import com.google.gson.Gson;



public class Template<M> {
	Class<M> clazz;
	Field1<M> idField;
	List<Field1<M>> dataFields;
	
	Template(Class<M> clazz) {
		this.clazz = clazz;
	}
	
	static abstract class Field1<M> {
		F2<ResultSet,Integer,Object> rsGet = null;
		P3<PreparedStatement, Integer, Object> psSetter = null;
		String sqlName = null;
		abstract void setValue(Object val, M m);
		abstract Object getValue(M m);
	}

	public static <M> Builder<M> builder(Class<M> clazz) {
		return new Builder<M>(clazz);
	}
	
	public static class Builder<M> {

		private Class<M> clazz;

		public Builder(Class<M> clazz) {
			this.clazz = clazz;
		}

		HashSet<String> dontStore = new HashSet<String>();
		public Template<M> build() {
			Template<M> template = new Template<M>(clazz);
			template.idField = field1(ReflectUtil.getField("id", clazz));
			template.dataFields = new LinkedList<>();
			eachField(clazz, (f) -> {
				if (dontStore.contains(f.getName())) {
					return;
				}
				template.dataFields.add(field1(f));
			});
			return template;
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
					ReflectUtil.setFieldValue(val, field, m);
				}
				@Override
				Object getValue(M m) {
					return ReflectUtil.getFieldValue(field, m);
				}
			};
			field1.sqlName = NameCaseUtil.camelToHyphen(field.getName());
			field1.psSetter = setter(field.getType());
			field1.rsGet = rsGet(field.getType());
			return field1;
		}

		Map<String,F1<Field1<M>,Field1<M>>> fieldDecors = new HashMap<>();
		public Builder<M> embededList(String fieldName, Class<?> elemCLass) {
			fieldDecors.put(fieldName, (f1) -> {
				Field1<M> newField1 = new Field1<M>() {
					@Override
					void setValue(Object val, M m) {
						if (val == null) {
							f1.setValue(null, m);
							return;
						}
						Object o = new Gson().fromJson(((String)val), ReflectUtil.forName("[L" + elemCLass.getName() + ";"));
						f1.setValue(ReflectUtil.invokeMethod("asList", new Object[] {o}, Arrays.class), m);
					}

					@Override
					Object getValue(M m) {
						Object val = f1.getValue(m);
						return new Gson().toJson(val);
					}
				};
				newField1.psSetter = setter(String.class);
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
	}

	public void insert(M m, Connection conn) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			List<Field1<M>> fields = dataFields;
			ps = conn.prepareStatement("INSERT INTO `" + cName() + "`(" + fieldNames(fields) + ") VALUES(" + fieldsPH(fields) + ")", Statement.RETURN_GENERATED_KEYS); // new String[] {"id"}
			psSet1(fields, m, ps);
			int result = ps.executeUpdate();
			if (result != 1) {
				throw new RuntimeException("Failed to insert record into " + cName() + " table");
			}
			
			rs = ps.getGeneratedKeys();
			if (rs.next()){
			    setId(m, rs.getLong(1));
			}
			
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtil.close(rs);
			IOUtil.close(ps);
		}
	}

	private String cName() {
		return clazz.getSimpleName().toLowerCase();
	}

	private void setId(M m, long id) {
		idField.setValue(id, m);
	}


	private void psSet1(Object[] params, PreparedStatement ps) throws SQLException {
		int index = 1;
		index = psSet(params, ps, index);
	}

	private int psSet(Object[] params, PreparedStatement ps, int index)
			throws SQLException {
		for (Object val : params) {
			if (val == null) {
				ps.setNull(index++, Types.INTEGER);
			} else {
				P3<PreparedStatement, Integer, Object> setter = setter(val.getClass());
				setter.e(ps, index++, val);
			}
		}
		return index;
	}
	private void psSet1(List<Field1<M>> fields, M m, PreparedStatement ps) throws SQLException {
		int index = 1;
		for (Field1 field : fields) {
			Object val = field.getValue(m);
//			Object val = ReflectUtil.getFieldValue(field, m);
			
			if (val == null) {
				ps.setNull(index++, Types.INTEGER);
			} else {
				P3<PreparedStatement, Integer, Object> setter = field.psSetter;
				setter.e(ps, index++, val);
			}
		}
	}

	private static P3<PreparedStatement, Integer, Object> setter(Class<?> type) {
		if (type.equals(Date.class)) {
			return (ps, index, val) -> {
				try {
					ps.setTimestamp(index, new Timestamp(((Date)val).getTime()));
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			};
		}
		if (type.equals(boolean.class)) {
			return (ps, index, val) -> {
				try {
					ps.setInt(index, ((Boolean)val).booleanValue() ? 1 : 0);
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			};
		}
		Method method = ReflectUtil.getMethod("set" + StringUtil.upperCaseFirstChar(type.getSimpleName()), PreparedStatement.class);
		if (method == null) {
			return null;
		}
		return (ps, index, val) -> {
			ReflectUtil.invoke(method, ps, new Object[] {index, val});
		};
	}

	private String fieldsPH(List<Field1<M>> fields) {
		return Cols.join(Cols.createList(fields.size(), () -> "?"), ",");
	}

	private String fieldNames(List<Field1<M>> fields) {
		return Cols.join(Cols.yield(fields, (f) -> f.sqlName), ",");
	}

	public void delete(Connection conn, String query, Object... params) {
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement("DELETE FROM `" + cName() + "` " + query);
			psSet1(params, ps);
			
			int result = ps.executeUpdate();
			if (result != 1) {
				throw new RuntimeException("Failed to delete record from " + cName() + " table");
			}
			
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtil.close(ps);
		}
	}
	
	public void update(Connection conn, String query, Object... params) {
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement("UPDATE `" + cName() + "` " + query);
			psSet1(params, ps);
			
			int result = ps.executeUpdate();
			if (result != 1) {
				throw new RuntimeException("Failed to update record into " + cName() + " table");
			}
			
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtil.close(ps);
		}
	}

	public void update(M m, Connection conn, String query, Object... params) {
		PreparedStatement ps = null;
		try {
			List<Field1<M>> fields = allFields();
			ps = conn.prepareStatement("UPDATE `" + cName() + "` SET " + psSetUpdate(fields) + " " + query);
			psSet1(fields, m, ps);
			psSet(params, ps, fields.size() + 1);

			int result = ps.executeUpdate();
			if (result != 1) {
				throw new RuntimeException("Failed to update record into " + cName() + " table");
			}
			
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtil.close(ps);
		}
	}

	private String psSetUpdate(List<Field1<M>> fields) {
		return Cols.join(Cols.yield(fields, (f) -> f.sqlName + "=?"), ",");
	}

	public M select(Connection conn, String query, Object... params) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			List<Field1<M>> allFields = allFields();
			ps = conn.prepareStatement("SELECT " + fieldNames(allFields) + " FROM `" + cName() + "` " + query);
			psSet1(params, ps);

			rs = ps.executeQuery();
			
			if (!rs.next()) {
				return null;
			}
			M m = ReflectUtil.newInstance(clazz);
			rsSet(allFields, m, rs);
			
			return m;
			
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtil.close(rs);
			IOUtil.close(ps);
		}
	}

	public List<M> selectAll(Connection conn) {
		List<Field1<M>> fields = allFields();
		String cond = "";
		return selectList(conn, fields, cond);
	}

	private List<M> selectList(Connection conn, List<Field1<M>> fields,
			String cond, Object... params) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement("SELECT " + fieldNames(fields) + " FROM `" + cName() + "` " + cond);
			psSet1(params, ps);
			rs = ps.executeQuery();
			
			LinkedList<M> list = new LinkedList<>();
			while (rs.next()) {
				M m = ReflectUtil.newInstance(clazz);
				rsSet(fields, m, rs);
				list.add(m);
			}
			
			return list;
			
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtil.close(rs);
			IOUtil.close(ps);
		}
	}

	private void rsSet(List<Field1<M>> fields, M m, ResultSet rs) {
		int index = 1;
		for (Field1 field : fields) {
			field.setValue(field.rsGet.e(rs, index++), m);
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
		Method methodGet = ReflectUtil.getMethod("get" + StringUtil.upperCaseFirstChar(type.getSimpleName()), new Class[] {int.class}, ResultSet.class);
		return (rs, index) -> {
			Object val = ReflectUtil.invoke(methodGet, rs, new Object[] {index});
			Boolean wasNull = ReflectUtil.invoke(methodWasNull, rs);
			
			return wasNull ? null : val;
		};
	}

	private List<Field1<M>> allFields() {
		LinkedList<Field1<M>> ret = new LinkedList<>();
		ret.add(idField);
		ret.addAll(dataFields);
		return ret;
	}

	private static void eachField(Class<?> clazz, P1<Field> p1) {
		for (final Field field : clazz.getDeclaredFields()) {
			int modifiers = field.getModifiers();
			if ((modifiers & (Modifier.STATIC | Modifier.FINAL | Modifier.TRANSIENT)) > 0
					|| (modifiers & Modifier.PUBLIC) == 0
					) {
				continue;
			}
			
			if (field.getName().equals("id")) {
				continue;
			}
			p1.e(field);
		}
	}

	public List<M> selectList(Connection conn, String query, Object... params) {
		Query<M> parseQuery = parseQuery(query);
		
		return selectList(conn, parseQuery.fields, parseQuery.cond, params);
	}
	
	private Query<M> parseQuery(String query) {
		Matcher matcher = RegexUtil.matcher("^(?i)SELECT (.+?) (?:FROM .+? )?(WHERE .+)$", query);
		if (!matcher.matches()) {
			throw new RuntimeException("Can not parse this query: " + query);
		}
		
		return new Query<M>(parseFields(matcher.group(1)), matcher.group(2));
	}
	
	public static void main(String[] args) {
		new Template<Aa>(Aa.class).parseQuery("SELECT receiver_name, receiver_address, receiver_phone WHERE order_by=? AND deliver_time > ? ORDER BY deliver_time DESC");
	}

	private List<Field1<M>> parseFields(String fields) {
		LinkedList<Field1<M>> ret = new LinkedList<>();
		for (String sqlName : fields.split("\\s*,\\s*")) {
			ret.add(getField(sqlName));
		}
		return ret;
	}

	private Field1<M> getField(String sqlName) {
		if (sqlName.equals("id")) {
			return idField;
		}
		for (Field1<M> field1 : dataFields) {
			if (field1.sqlName.equals(sqlName)) {
				return field1;
			}
		}
		throw new RuntimeException("Can not find this field: " + sqlName);
	}

	static class Query<M> {
		List<Field1<M>> fields;
		String cond;
		public Query(List<Field1<M>> fields, String cond) {
			this.fields = fields;
			this.cond = cond;
		}
		
	}
}
