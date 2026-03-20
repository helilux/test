#include "matching_engine.h"

extern int bar_volume_fill_ratio; 
extern std::unordered_map<int, int> LotdataMap;

std::string get_dump_filename(long timestamp) {
    std::time_t raw_time = static_cast<std::time_t>(timestamp);
    struct tm * timeinfo = std::localtime(&raw_time);
    
    char buffer[80];
    
    std::string path = "/data.path/update_v2";
    
    struct stat st = {0};
    std::string dir_check = path.substr(0, path.size()-1); 
    if (stat(dir_check.c_str(), &st) == -1) {
        // Directory doesn't exist handling (optional)
    }

    std::strftime(buffer, 80, "NSE_FO_cpp_dump_%y%m%d.bin", timeinfo);
    return path + std::string(buffer);
}


bool load_dump_from_file(const std::string& filename, std::unordered_map<long, std::unordered_map<int, MarketData>>& data_map) {
    std::ifstream in(filename, std::ios::binary);
    if (!in) return false;

    size_t map_size = 0;
    in.read(reinterpret_cast<char*>(&map_size), sizeof(map_size));

    for (size_t i = 0; i < map_size; ++i) {
        long ts;
        in.read(reinterpret_cast<char*>(&ts), sizeof(ts));

        size_t inner_size = 0;
        in.read(reinterpret_cast<char*>(&inner_size), sizeof(inner_size));

        for (size_t j = 0; j < inner_size; ++j) {
            // Initialize with defaults to avoid garbage values
            MarketData md; 
            
            in.read(reinterpret_cast<char*>(&md.token), sizeof(md.token));
            in.read(reinterpret_cast<char*>(&md.timestamp), sizeof(md.timestamp));
            in.read(reinterpret_cast<char*>(&md.open), sizeof(md.open));
            in.read(reinterpret_cast<char*>(&md.high), sizeof(md.high));
            in.read(reinterpret_cast<char*>(&md.low), sizeof(md.low));
            in.read(reinterpret_cast<char*>(&md.close), sizeof(md.close));
            in.read(reinterpret_cast<char*>(&md.volume), sizeof(md.volume));
            in.read(reinterpret_cast<char*>(&md.vwap), sizeof(md.vwap));
            in.read(reinterpret_cast<char*>(&md.orig_volume), sizeof(md.orig_volume));

            size_t bid_sz = 0;
            in.read(reinterpret_cast<char*>(&bid_sz), sizeof(bid_sz));
            md.bidLevels.resize(bid_sz); // Resizes vector of OrderBookLevel
            if(bid_sz > 0)
                // CRITICAL FIX: Read correct size for OrderBookLevel
                in.read(reinterpret_cast<char*>(md.bidLevels.data()), bid_sz * sizeof(OrderBookLevel));

            size_t ask_sz = 0;
            in.read(reinterpret_cast<char*>(&ask_sz), sizeof(ask_sz));
            md.askLevels.resize(ask_sz); // Resizes vector of OrderBookLevel
            if(ask_sz > 0)
                // CRITICAL FIX: Read correct size for OrderBookLevel
                in.read(reinterpret_cast<char*>(md.askLevels.data()), ask_sz * sizeof(OrderBookLevel));

            data_map[ts][md.token] = md;
        }
    }
    in.close();
    printf("--- Loaded %lu timestamps from C++ binary dump: %s ---\n", data_map.size(), filename.c_str());
    return true;
}


std::time_t calculateEpochTime(const std::string& date) {

	std::tm timeinfo = {};

	std::istringstream ss(date);
	char delimiter;
	ss >> timeinfo.tm_year >> delimiter >> timeinfo.tm_mon >> delimiter >> timeinfo.tm_mday;

	timeinfo.tm_year -= 1900;
	timeinfo.tm_mon -= 1;

	timeinfo.tm_hour = 9;
	timeinfo.tm_min = 15;
	timeinfo.tm_sec = 0;


	std::time_t epochTime = std::mktime(&timeinfo);

	return epochTime;
}

MatchingEngine::MatchingEngine(std::ofstream *logFile, int interval) : barInterval(interval) {
	currentTimestamp = 0;
	mysqlAccess = new MysqlAccess("192.168.36.110", "root", "root123", "nse_Backtester_data", logFile);
	conn = mysqlAccess->getConnection();
//	std::time_t epoch_time = calculateEpochTime("2024-08-20");
//	currentTimestamp = static_cast<int64_t>(epoch_time) + 19800;
//	printf("currentTimestamp = %d \n",currentTimestamp);
}

MatchingEngine::~MatchingEngine() {
	delete mysqlAccess; 
}

/*
void MatchingEngine::loadMarketData(int token) {
	char query[1024];
	snprintf(query, sizeof(query),
			"SELECT timestamp, token, open, high, low, close, volume, vwap,"
			"bpx1, bqty1, bpx2, bqty2, bpx3, bqty3, bpx4, bqty4, bpx5, bqty5, "
			"spx1, sqty1, spx2, sqty2, spx3, sqty3, spx4, sqty4, spx5, sqty5 "
			"FROM Market_Data_Snapshots "
			"WHERE token = '%d' AND timestamp >= %ld "
			"ORDER BY timestamp ASC LIMIT 1",
			token, currentTimestamp);

	if (mysql_query(conn, query)) {
		std::cerr << "MySQL query error: " << mysql_error(conn) << std::endl;
		return;
	}

	MYSQL_RES* res = mysql_store_result(conn);
	if (res == nullptr) {
		std::cerr << "MySQL store result error: " << mysql_error(conn) << std::endl;
		return;
	}

	MYSQL_ROW row;
	if ((row = mysql_fetch_row(res))) {
		MarketData data(
				row[1],                    
				std::stol(row[0]),      
				std::stoi(row[2]),        
				std::stoi(row[3]),       
				std::stoi(row[4]),        
				std::stoi(row[5]),        
				std::stoi(row[6]),
                                std::stoi(row[7])         
			       );

		for (int i = 0; i < 5; i++) {
			int32_t price = std::stoi(row[8 + i*2]);
			int qty = std::stoi(row[9 + i*2]);
			data.bidLevels.emplace_back(price, qty);
		}

		for (int i = 0; i < 5; i++) {
			int32_t price = std::stoi(row[18 + i*2]);
			int qty = std::stoi(row[19 + i*2]);
			data.askLevels.emplace_back(price, qty);
		}

//		marketData[token] = data;
                marketData.insert(std::make_pair(token, std::move(data)));
//		currentTimestamp = std::stol(row[0]);
	}

	mysql_free_result(res);
}
*/


long long safe_stoll(const std::string& str) {
    try {
        return std::stoll(str);
    } catch (const std::invalid_argument&) {
        return 0LL; // Suffix LL for long long
    } catch (const std::out_of_range&) {
        return 0LL;
    }
}


int safe_stoi(const std::string& str) {
    try {
        return std::stoi(str);
    } catch (const std::invalid_argument&) {
        return 0;
    } catch (const std::out_of_range&) {
        return 0;
    }
}



void MatchingEngine::pickle_market_data(uint32_t starting_timestamp, uint32_t ending_timestamp, const std::string &tokens_file) {
    
    // 1. GENERATE FILE PATH (DDMMYY format)
    time_t rawtime = (time_t)starting_timestamp;
    struct tm * timeinfo = localtime(&rawtime);
    char date_buffer[10];
    strftime(date_buffer, 10, "%d%m%y", timeinfo);
    
    std::string csv_file_path = "/mnt/huge_disk/NSE/market_data/fo_files/market_data_FO_" + std::string(date_buffer) + ".csv";
    
    printf("--- Streaming Market Data from CSV: %s ---\n", csv_file_path.c_str());

    // 2. LOAD TOKENS
    std::set<int> interest_tokens;
    std::ifstream t_file(tokens_file);
    std::string token_str;

    if (t_file.is_open()) {
        while (t_file >> token_str) {
            if (std::all_of(token_str.begin(), token_str.end(), ::isdigit)) {
                interest_tokens.insert(std::stoi(token_str));
            }
        }
        t_file.close();
    }

    if (interest_tokens.empty()) {
        // Return or throw error based on your preference
        std::cerr << "No valid tokens found." << std::endl;
        return; 
    }

    // 3. READ CSV FILE
    std::ifstream file(csv_file_path);
    if (!file.is_open()) {
        std::cerr << "CSV " << csv_file_path << " not found." << std::endl;
        return;
    }

    std::string line;
    std::getline(file, line); // Skip Header

    long long count = 0;
    std::string cell;
    std::vector<std::string> row;
    row.reserve(40); 

    while (std::getline(file, line)) {
        std::stringstream lineStream(line);
        row.clear();
        
        while (std::getline(lineStream, cell, ',')) {
            row.push_back(cell);
        }

        if (row.size() < 30) continue; 

        // A. Timestamp Filter
        long timestamp = std::stol(row[0]);
        if (timestamp < starting_timestamp) continue;
        if (timestamp > ending_timestamp) break; 

        // B. Token Filter
        int token = std::stoi(row[2]);
        if (interest_tokens.find(token) == interest_tokens.end()) {
            continue;
        }

        // C. Parse Values
        int lotsize = (token > 0) ? LotdataMap[token] : 0;
        long long volume = (!row[9].empty()) ? std::stoll(row[9]) : 0;
        long long vwap_val = (!row[10].empty()) ? std::stoll(row[10]) : 0;

        if (vwap_val != 0 && volume != 0) {
            vwap_val = (vwap_val / volume);
        } else {
            vwap_val = 0;
        }

        if (volume > 0 && lotsize > 0) {
            long long temp_vol = volume / lotsize;
            volume = (temp_vol >= 1) ? temp_vol : 0; 
        }

        MarketData market_data(
            token,                      
            timestamp,
            (!row[4].empty()) ? std::stoi(row[4]) : 0,  // open
            (!row[5].empty()) ? std::stoi(row[5]) : 0,  // high
            (!row[6].empty()) ? std::stoi(row[6]) : 0,  // low
            (!row[7].empty()) ? std::stoi(row[7]) : 0,  // close                  
            volume,                     
            vwap_val         
        );

        for (int i = 0; i < 5; ++i) {
            double price = std::stod(row[11 + (i * 2)]);
            int qty = std::stoi(row[12 + (i * 2)]);
            market_data.bidLevels.emplace_back((int)price, qty); 
        }

        for (int i = 0; i < 5; ++i) {
            double price = std::stod(row[21 + (i * 2)]);
            int qty = std::stoi(row[22 + (i * 2)]);
            market_data.askLevels.emplace_back((int)price, qty);
        }

        // E. Store in Map
        timestamp_market_data_map[timestamp][market_data.token] = market_data;
        count++;
    }

    file.close();
    printf("--- Successfully loaded %lu timestamps (Total rows: %lld) from CSV ---\n", timestamp_market_data_map.size(), count);
}

int MatchingEngine::ModifyOrder(int32_t clOrdNum, int32_t Price, int32_t qty, int32_t Token, int32_t BuySell) {

 	int quantity = 0;

	if (BuySell == 1) { 
		auto range = buyOrders.equal_range(clOrdNum);
		for (auto it = range.first; it != range.second; ++it) {
			if (it->second.token == Token) {
				if (it->second.quantity < qty) {
					it->second.originalQuantity = qty;
					it->second.price = Price;
				}
			}
		}
	} else if (BuySell == 2) { 
		auto range = sellOrders.equal_range(clOrdNum);
		for (auto it = range.first; it != range.second; ++it) {
			if (it->second.token == Token) {
				if (it->second.quantity < qty) {
					it->second.originalQuantity = qty;
					it->second.price = Price;
				}
			}
		}
	}

 	return quantity;
}

void MatchingEngine::addOrder(const Order& order) {
//	printf("adding order \n");
	if (order.type == 1) {
//		printf("adding buy order order.token = %d \n", order.token);
		buyOrders.insert({ order.orderNumber, order });
	} 
	else if (order.type == 2) {
//		printf("adding sell order order.token = %d \n", order.token);
		sellOrders.insert({ order.orderNumber, order });
	}
}

void MatchingEngine::removeOrdersForClordNum(int orderNumber) {
//        printf("deleting orderno = %d \n", orderNumber);
	buyOrders.erase(orderNumber);
	sellOrders.erase(orderNumber);
}

void convertDateToTimestamp(long currentTimestamp){

	std::time_t time = static_cast<std::time_t>(currentTimestamp - 19800);

	std::tm* localTime = std::localtime(&time);
  
	printf("BAR TIME: %04d-%02d-%02d %02d:%02d:%02d\n", 
			localTime->tm_year + 1900, 
			localTime->tm_mon + 1,    
			localTime->tm_mday,        
			localTime->tm_hour,        
			localTime->tm_min,       
			localTime->tm_sec); 
}

void MatchingEngine::matchOrders() {

        convertDateToTimestamp(currentTimestamp);
//	while ((!buyOrders.empty() || !sellOrders.empty()) && !(*stop_flag)) {

		if(!buyOrders.empty()){
			for (auto highestBuy = buyOrders.begin(); highestBuy != buyOrders.end(); ) {


				MarketData* currentData = nullptr;
//                                currentData = &timestamp_market_data_map[currentTimestamp][highestBuy->second.token];

                                if (timestamp_market_data_map.find(currentTimestamp) != timestamp_market_data_map.end()) {
                                        auto& innerMap = timestamp_market_data_map[currentTimestamp];
                                        if (innerMap.find(highestBuy->second.token) != innerMap.end()) {
                                                currentData = &innerMap[highestBuy->second.token];

                                        }

                                }

 
				if (currentData) {

					if (1) {
                                               
                                                int matchedQuantity = highestBuy->second.quantity; 

                                                if(currentData->volume >= matchedQuantity && ((currentData->volume - matchedQuantity) > (currentData->orig_volume / bar_volume_fill_ratio)) && highestBuy->second.price == 0) {
//                                                        printf("matchedQuantity 1 is %d \n", matchedQuantity);
                                                	currentData->volume -= matchedQuantity;
                                                }
						else if(highestBuy->second.price == 0) {

							if(currentData->volume > 0 && (currentData->orig_volume / bar_volume_fill_ratio) < currentData->volume) {
                                                                matchedQuantity = static_cast<int>(std::round(currentData->volume - (currentData->orig_volume / bar_volume_fill_ratio)));
                                                                currentData->volume -= matchedQuantity;
//                                                                printf("matchedQuantity 2 is %d \n", matchedQuantity);
                                                        }
                                                        else{
                                                                 matchedQuantity = 0;
                                                                 ++highestBuy;
                                                                 continue;
                                                        }
                                                }
                                                else if (highestBuy->second.price > 0 && highestBuy->second.price > currentData->vwap) {


                                                        if(currentData->volume >= matchedQuantity && ((currentData->volume - matchedQuantity) > (currentData->orig_volume / bar_volume_fill_ratio))) {
                                                                currentData->volume -= matchedQuantity;
                                                        }
                                                        else if(currentData->volume > 0 && (currentData->orig_volume / bar_volume_fill_ratio) < currentData->volume) {
                                                                matchedQuantity = static_cast<int>(std::round(currentData->volume - (currentData->orig_volume / bar_volume_fill_ratio)));
                                                                currentData->volume -= matchedQuantity;
                                                        }
                                                        else{
                                                                 matchedQuantity = 0;
                                                                 ++highestBuy;
                                                                 continue;
                                                        }

                                                } else {
                                                        ++highestBuy;
                                                        continue;
                                                }

						double fillPrice = currentData->vwap;

						MS_TRADE_CONFIRM_TR buyerConfirm = st_cal->createTradeConfirmation(
								highestBuy->second, fillPrice, matchedQuantity);
						orderReceiver->sendTradeConfirmation(buyerConfirm, highestBuy->second.orderNumber);

						highestBuy->second.quantity -= matchedQuantity;

						if (highestBuy->second.quantity == 0) {
							highestBuy = buyOrders.erase(highestBuy);
						} else {

							++highestBuy;
						}
					} else {
						++highestBuy;
					}

				}
				else{
				        printf("data not found for token %d, timestaamp = %d\n", highestBuy->second.token, currentTimestamp);	
					++highestBuy;
				}
			}
		}


		if(!sellOrders.empty()){
			for (auto lowestSell =  sellOrders.begin(); lowestSell != sellOrders.end(); ) {

				MarketData* currentData = nullptr;
//                                currentData = &timestamp_market_data_map[currentTimestamp][lowestSell->second.token];

                                if (timestamp_market_data_map.find(currentTimestamp) != timestamp_market_data_map.end()) {
    					auto& innerMap = timestamp_market_data_map[currentTimestamp];
    					if (innerMap.find(lowestSell->second.token) != innerMap.end()) {
        					currentData = &innerMap[lowestSell->second.token];

    					}

				} 

                               

				if (currentData) {

                                       // std::cout << "current_data volume is " << currentData->volume << std::endl;


						int matchedQuantity = lowestSell->second.quantity;

                                                if(currentData->volume >= matchedQuantity && ((currentData->volume - matchedQuantity) > (currentData->orig_volume / bar_volume_fill_ratio)) && lowestSell->second.price == 0) {
                                                        currentData->volume -= matchedQuantity;
                                                }
                                                else if(lowestSell->second.price == 0) {

                                                        if(currentData->volume > 0 && (currentData->orig_volume / bar_volume_fill_ratio) < currentData->volume) {
                                                        	matchedQuantity = static_cast<int>(std::round(currentData->volume - (currentData->orig_volume / bar_volume_fill_ratio)));
                                                                currentData->volume -= matchedQuantity;
                                                        }
                                                        else{
                                                                 matchedQuantity = 0;
       								 ++lowestSell;
                                                                 continue;
							}
                                                }
                                                else if (lowestSell->second.price > 0 && lowestSell->second.price < currentData->vwap) {
                                                        

                                                        if(currentData->volume >= matchedQuantity && ((currentData->volume - matchedQuantity) > (currentData->orig_volume / bar_volume_fill_ratio))) {
                                                        	currentData->volume -= matchedQuantity;
                                                	}
                                                	else if(currentData->volume > 0 && (currentData->orig_volume / bar_volume_fill_ratio) < currentData->volume) {
                                                                matchedQuantity = static_cast<int>(std::round(currentData->volume - (currentData->orig_volume / bar_volume_fill_ratio)));
                                                                currentData->volume -= matchedQuantity;
                                                	}
                                                        else{
                                                                 matchedQuantity = 0;
                                                                 ++lowestSell;
                                                                 continue;
                                                        }
                                 
 
                                                        

                                                } else {
                                                	++lowestSell;
                                                        continue;
                                        	}

 
						double fillPrice = currentData->vwap;

						MS_TRADE_CONFIRM_TR sellerConfirm = st_cal->createTradeConfirmation(
								lowestSell->second, fillPrice, matchedQuantity);
						orderReceiver->sendTradeConfirmation(sellerConfirm, lowestSell->second.orderNumber);

						lowestSell->second.quantity -= matchedQuantity;

						if (lowestSell->second.quantity == 0) {
							lowestSell = sellOrders.erase(lowestSell);
						} else {
							++lowestSell;
						}
				}
				else{
					printf("data not found for token %d, timestaamp = %d\n", lowestSell->second.token, currentTimestamp);
					++lowestSell;
				}
			}
		}
//	}

}
