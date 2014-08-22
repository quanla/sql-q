package qj.tool.sql.test;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

import qj.util.ReflectUtil;

public class Aa {
	public Long id;
	public String name;
	public int age;
	public Date created;
	public List<Food> foods;
	
	public static class Food {
		public BigDecimal cal;

		public Food(double cal) {
			this.cal = new BigDecimal(cal);
		}

		@Override
		public String toString() {
			return "Food [cal=" + cal + "]";
		}
	}
	
//	public static void main(String[] args) throws ClassNotFoundException {
////		System.out.println(ReflectUtil.forName("[L"+Food.class.getName()));
//		System.out.println(Food[].class.getName());
//		System.out.println(ReflectUtil.forName(Food[].class.getName()));
//	}
	
	@Override
	public String toString() {
		return "User [id=" + id + ", name=" + name + ", age=" + age + ", created=" + created + ", foods=" + foods + "]";
	}
}
