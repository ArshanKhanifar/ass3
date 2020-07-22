service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  void receive_values_from_primary(1: list<string> keys, 2: list<string> values);
  void debug_get_everything_from_primary(1: list<string> keys, 2: list<string> values);
}
