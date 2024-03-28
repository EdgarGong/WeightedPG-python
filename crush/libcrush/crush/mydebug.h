#include <iostream>

std::ostream& operator<<(std::ostream& os, const set<int>& s) {
    os << "{";
    for (auto v : s) {
        os << v << ",";
    }
    os << "}";
    return os;
}

std::ostream& operator<<(std::ostream& os, const vector<int>& v) {
    os << "{";
    for (auto val : v) {
        os << val << ",";
    }
    os << "}";
    return os;
}

std::ostream& operator<<(std::ostream& os, const vector<pair<int,int>>& pv) {
    os << "{";
    for (auto& p : pv) {
        os << "{" << p.first << "," << p.second << "}" << ",";
    }
    os << "}";
    return os;
}