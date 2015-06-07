# sql-q
A simple, lightweight JDBC ORM for Java, built to be as similar to normal SQL as possible, suitable for RESTful services.

No support for Joins, but you can always use normal SQL queries for them.

## Usage

Model class:
    
	public class Item {
		// By default, id is primary key, but you can declare custom id columns or no id at all when you build the template
		public Long id;
        
		public String name;
		public BigDecimal giaNhap;
		public Map<String,BigDecimal> sellPrices;
	
		public String barcode;
		
		public boolean disabled = false;
		public Date disabledSince;
		
		public Date createdDate;
	
		public String unit;
		
		// No need for getters, setters
	}

ItemDAO class:

	public class ItemDAO {
		public static Template<Item> template = Template.builder(Item.class)
				.embeded("sellPrices")
				.build();
	
		public static List<Item> getAllItems( Connection conn ) {
			return template.selectAll(conn);
		}
	
		public static List<Item> getActiveItems( Connection conn ) {
			return template.selectList(conn, "WHERE disabled=?", false);
		}

		public static boolean existName(String name, Connection conn) {
			return template.exists(conn, "WHERE LOWER(name)=?", name.toLowerCase());
		}
	
		public static List<Item> getItemNames(Connection conn) {
			return template.selectList(conn, "SELECT id, name");
		}
	
		public static List<Item> getDisabledItems( Connection conn ) {
			return template.selectList(conn, "WHERE disabled=?", true);
		}
	
		public static Item get(Long id, Connection conn) {
			return template.selectById(id, conn);
		}
	
		public static void update(Item item, Connection conn) {
			template.update(item, conn);
		}
	
		public static void insert(Item item, Connection conn) {
			template.insert(item, conn);
		}
	}
