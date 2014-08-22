package qj.tool.sql.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import qj.tool.sql.Template;
import qj.tool.sql.test.Aa.Food;

public class UserDB {
	static Template<Aa> template = Template.builder(Aa.class)
				.embededList("foods", Food.class)
				.build();
	
	public static void insert(Aa user, Connection conn) {
		template.insert(user, conn);
	}
	
	public static void delete(Long id, Connection conn) {
		template.delete(conn, "where id=?", id);
	}
	
	public static void update(Aa user, Connection conn) {
		template.update(user, conn, "where id=?", user.id);
	}
	
	public static Aa select(Long id, Connection conn) {
		return template.select(conn, "where id=?", id);
	}
	

	public static List<Aa> selectAll(Connection conn) {
		return template.selectAll(conn);
	}
	
	public static void main(String[] args) throws Exception {
		
		
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		
		Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/test?" +
		                                   "user=quan&password=qweqweqwe");

		testInsert(conn);
//		testDelete(2L, conn);
//		testUpdate(conn);
		for (Aa user : UserDB.selectAll(conn)) {
			System.out.println(user);
		}
		
//		System.out.println(UserDB.select(3L, conn));
		
		conn.close();
		
	}

	private static void testDelete(long id, Connection conn) {
		UserDB.delete(id, conn);
	}

	@SuppressWarnings("unused")
	private static void testInsert(Connection conn) {
		Aa user = new Aa();
		user.name = "Le Anh Quan";
		user.age = 30;
		user.foods = Arrays.asList(new Food(1.2));
		user.created = new Date();
		UserDB.insert(user, conn);
		System.out.println(user.id);
	}
	
//	@SuppressWarnings("unused")
	private static void testUpdate(Connection conn) {
		Aa user = UserDB.select(3L, conn);
		user.age = 33;
		UserDB.update(user, conn);
		user = UserDB.select(3L, conn);
		System.out.println(user.age);
	}
}
