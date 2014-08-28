package qj.tool.sql;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.Map;

import qj.tool.sql.test.Aa;
import qj.util.Cols;
import qj.util.IOUtil;
import qj.util.NameCaseUtil;
import qj.util.ReflectUtil;
import qj.util.StringUtil;
import qj.util.funct.F1;
import qj.util.funct.P0;
import qj.util.funct.P1;
import qj.util.funct.P3;
import qj.util.template.Template;

public class SQLUtil {
	
	public static void main(String[] args) {
		System.out.println(SQLUtil.generate(Aa.class));
	}
	public static String generate(Class<?> clazz) {
		
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		generate(clazz, bout);
		return bout.toString();
	}
	
	private static void generate(Class<?> clazz, OutputStream out1) {
		
		StringBuilder fieldNames = new StringBuilder();
		StringBuilder fieldPH = new StringBuilder();
		StringBuilder psSetUpdate = new StringBuilder();
		StringBuilder rsSet1 = new StringBuilder();
		StringBuilder rsSet2 = new StringBuilder();
		StringBuilder psSet1 = new StringBuilder();
		StringBuilder psSet2 = new StringBuilder();
		String cName = StringUtil.lowerCaseFirstChar(clazz.getSimpleName());
		int[] count = {0};
		
		eachField(clazz, (field) -> {
			if (fieldNames.length() > 0) {
				fieldNames.append(", ");
				fieldPH.append(", ");
				psSetUpdate.append(", ");
			}
			count[0]++;
			
			String sql_field_name = NameCaseUtil.camelToHyphen(field.getName());
			fieldNames.append("`" + sql_field_name + "`");
			fieldPH.append("?");
			psSetUpdate.append(sql_field_name + "=?");
			String jdbcType = jdbcType(field.getType());
			
//			order.orderBy = rs.wasNull() ? null : order.orderBy;
			
			rsSet1.append("\t\t\t" + cName + "." + field.getName() + " = " + jdbcValToModel("rs.get" + jdbcType + "(" + (count[0] + 0) + ")", field.getType()) + ";\n");
			rsSet2.append("\t\t\t\t" + cName + "." + field.getName() + " = " + jdbcValToModel("rs.get" + jdbcType + "(" + (count[0] + 1) + ")", field.getType()) + ";\n");
			if (checkNull(field)) {
				rsSet1.append("\t\t\t" + cName + "." + field.getName() + " = rs.wasNull() ? null : " + cName + "." + field.getName() + ";\n");
				rsSet2.append("\t\t\t\t" + cName + "." + field.getName() + " = rs.wasNull() ? null : " + cName + "." + field.getName() + ";\n");
			}
			
			String jdbcVal = jdbcVal(cName + "." + field.getName(), field.getType());
			if (checkNull(field)) {
				F1<Integer,String> set = c -> {
					StringBuilder sb = new StringBuilder();
					sb.append("\t\t	if (" + jdbcVal + " == null) {\n");
					sb.append("\t\t		ps.setNull(" + c + ", Types.INTEGER);\n");
					sb.append("\t\t	} else {\n");
					sb.append("\t\t		ps.set" + jdbcType + "(" + c + ", " + jdbcVal + ");\n");
					sb.append("\t\t	}\n");
					return sb.toString();
				};
				psSet1.append(set.e(count[0] + 0));
				psSet2.append(set.e(count[0] + 1));
			} else {
				psSet1.append("\t\t\tps.set" + jdbcType + "(" + (count[0] + 0) + ", " + jdbcVal + ");\n");
				psSet2.append("\t\t\tps.set" + jdbcType + "(" + (count[0] + 1) + ", " + jdbcVal + ");\n");
			}
			
		});
		
		Map<String, Object> vars = Cols.map(
				"CName", clazz.getSimpleName(),
				"cName", cName,
				"psSetUpdate", psSetUpdate.toString(),
				"fieldNames", fieldNames.toString(),
				"fieldPH", fieldPH.toString(),
				"rsSet1", rsSet1.toString(),
				"rsSet2", rsSet2.toString(),
				"psSet1", psSet1.toString(),
				"psSet2", psSet2.toString(),
				"count1", count[0]+1,
				"classFullName", clazz.getName()
				);

		PrintWriter out = new PrintWriter(out1);
		
		Template.compileSimple(SQLUtil.class, "crud.tmpl").write(vars, out);
		
		IOUtil.close(out);
	}
	private static boolean checkNull(Field field) {
		return !field.getName().equals("id") && field.getType().equals(Long.class);
	}
	private static String jdbcValToModel(String val, Class<?> type) {
		if (type.equals(boolean.class)) {
			return val + " == 1 ? true : false";
		}
		return val;
	}
	private static String jdbcVal(String val, Class<?> type) {
		if (type.equals(Date.class)) {
			return "new Timestamp(" + val + ".getTime())";
		}
		if (type.equals(boolean.class)) {
			return val + " ? 1 : 0";
		}
		return val;
	}
	private static String jdbcType(Class<?> type) {
		if (type.equals(Date.class)) {
			return "Timestamp";
		}
		if (type.equals(boolean.class)) {
			return "Int";
		}
		return StringUtil.upperCaseFirstChar(type.getSimpleName());
	}

	public static long nextId(String tbl, Connection conn) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			ps = conn.prepareStatement("SELECT max(id) FROM " + tbl);
			rs = ps.executeQuery();
			
			rs.next();
			return rs.getLong(1);
			
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtil.close(rs);
			IOUtil.close(ps);
		}
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
	public static void transaction(P0 run, Connection conn) {
		try {
			conn.setAutoCommit(false);
			run.e();
			conn.commit();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
			}
			throw new RuntimeException(e);
		}
	}
	public static Long selectLong(Connection conn, String query) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(query);
			rs = ps.executeQuery();
			if (rs.next()) {
				return rs.getLong(1);
			}
			return null;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtil.close(rs);
			IOUtil.close(ps);
		}
	}
	public static void update(Connection conn, String query, Object... params) {
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(query);
			psSet1(params, ps);
			ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtil.close(ps);
		}
	}
	public static P3<PreparedStatement, Integer, Object> setter(Class<?> type) {
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
	static int psSet(Object[] params, PreparedStatement ps, int index)
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
	static void psSet1(Object[] params, PreparedStatement ps) throws SQLException {
		int index = 1;
		index = psSet(params, ps, index);
	}
}
