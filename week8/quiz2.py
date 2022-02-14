sales_1 = sales.select('date', 'store', 'sales')
sales_b = sales_1.filter('store' == 'Burnaby').select('date', sales_1['sales'].alias('Burnaby')).orderBy('date') 
sales_v = sales_1.filter('store' == 'Vancouver').select('date', sales_1['sales'].alias('Vancouver')).orderBy('date') 
comparison = sales_b.join(sales_v, ['date'])


def is_wkend(date):
	num = functions.dayofweek(new_sales['date']) 
	if num == 1 or num == 7:
		return True
	else:
		return False


def is_h(h):
	if h:
		return True
	else:
		return False

new_sales = sales.withColumn('sales_per_staff', sales['sales']/sales['staff']).orderBy('date')
new_sales_1 = new_sales_1.join(holidays, ['date'], 'left').orderBy('date')
new_sales_2 = new_sales_1.withColumn('is_holiday', is_h(new_sales_1['holiday']).orderBy('date'))
new_sales_3 = new_sales_1.withColumn('is_weekend', is_wkend(new_sales_2['date']).orderBy('date') )
features = new_sales_3.select('date', 'store', 'sales', 'sales_per_staff', 'is_holiday', 'is_weekend')
