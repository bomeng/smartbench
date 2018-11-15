export DATA_HDFS=$1

echo "Insert data into tpcds.call_center"
spark-submit call_center.py;

echo "Insert data into tpcds.catalog_page"
spark-submit catalog_page.py;

echo "Insert data into tpcds.catalog_returns"
spark-submit catalog_returns.py;

echo "Insert data into tpcds.catalog_sales"
spark-submit catalog_sales.py;

echo "Insert data into tpcds.customer_address"
spark-submit customer_address.py;

echo "Insert data into tpcds.customer_address"
spark-submit customer_demographics.py;

echo "Insert data into tpcds.customer_address"
spark-submit customer.py;

echo "Insert data into tpcds.date_dim"
spark-submit date_dim.py;

echo "Insert data into tpcds.household_demographics"
spark-submit household_demographics.py;

echo "Insert data into tpcds.income_band"
spark-submit income_band.py;

echo "Insert data into tpcds.inventory"
spark-submit inventory.py;

echo "Insert data into tpcds.item"
spark-submit item.py;

echo "Insert data into tpcds.promotion"
spark-submit promotion.py;

echo "Insert data into tpcds.reason"
spark-submit reason.py;

echo "Insert data into tpcds.ship_mode"
spark-submit ship_mode.py;

echo "Insert data into tpcds.store"
spark-submit store.py;

echo "Insert data into tpcds.store_returns"
spark-submit store_returns.py;

echo "Insert data into tpcds.store_sales"
spark-submit store_sales.py;

echo "Insert data into tpcds.time_dim"
spark-submit time_dim.py;

echo "Insert data into tpcds.warehouse"
spark-submit warehouse.py;

echo "Insert data into tpcds.web_page"
spark-submit web_page.py;

echo "Insert data into tpcds.web_returns"
spark-submit web_returns.py;

echo "Insert data into tpcds.web_sales"
spark-submit web_sales.py;

echo "Insert data into tpcds.web_site"
spark-submit web_site.py;
