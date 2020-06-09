# Downlaod data
dt="2020-05-30"
./bin/fetch_table_data.sh \
	./data/${dt}/tr_data \
	smartad-dmp/warehouse/user/seanchuang/i2i_offline_train_raw/dt=${dt}

./bin/fetch_table_data.sh \
	./data/${dt}/te_data \
	smartad-dmp/warehouse/user/seanchuang/i2i_offline_test_raw/dt=${dt}

# Process Data
