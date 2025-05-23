#include <mpi.h>
#include <omp.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <vector>
#include <string>
#include <algorithm>
#include <cstring>

using namespace std;

typedef unordered_map<string, int> WordCountMap;

vector<string> tokenize(const string& text) {
    vector<string> words;
    istringstream iss(text);
    string word;
    while (iss >> word) {
        word.erase(remove_if(word.begin(), word.end(), ::ispunct), word.end());
        transform(word.begin(), word.end(), word.begin(), ::tolower);
        if (!word.empty()) words.push_back(word);
    }
    return words;
}

WordCountMap count_words(const vector<string>& lines) {
    WordCountMap local_map;
    #pragma omp parallel
    {
        WordCountMap private_map;
        #pragma omp for nowait
        for (size_t i = 0; i < lines.size(); ++i) {
            vector<string> words = tokenize(lines[i]);
            for (const string& word : words) {
                private_map[word]++;
            }
        }
        #pragma omp critical
        for (auto& pair : private_map) {
            local_map[pair.first] += pair.second;
        }
    }
    return local_map;
}

void merge_maps(WordCountMap& a, const WordCountMap& b) {
    for (const auto& pair : b) {
        a[pair.first] += pair.second;
    }
}

string serialize_lines(const vector<string>& lines) {
    ostringstream oss;
    for (const string& line : lines) {
        oss << line << "\n";
    }
    return oss.str();
}

vector<string> split_lines(const string& str) {
    vector<string> lines;
    istringstream iss(str);
    string line;
    while (getline(iss, line)) {
        lines.push_back(line);
    }
    return lines;
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    double start_time = MPI_Wtime();

    vector<string> all_lines;
    vector<string> local_lines;

    if (rank == 0) {
        if (argc < 2) {
            cerr << "Usage: " << argv[0] << " <filename>\n";
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        ifstream infile(argv[1]);
        string line;
        while (getline(infile, line)) {
            all_lines.push_back(line);
        }
    }

    int total_lines = all_lines.size();
    MPI_Bcast(&total_lines, 1, MPI_INT, 0, MPI_COMM_WORLD);

    int base = total_lines / size;
    int extra = total_lines % size;
    int start = rank * base + min(rank, extra);
    int count = base + (rank < extra ? 1 : 0);

    if (rank == 0) {
        for (int i = 1; i < size; ++i) {
            int s = i * base + min(i, extra);
            int c = base + (i < extra ? 1 : 0);
            string chunk = serialize_lines(vector<string>(all_lines.begin() + s, all_lines.begin() + s + c));
            int len = chunk.length();
            MPI_Send(&len, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
            MPI_Send(chunk.c_str(), len, MPI_CHAR, i, 1, MPI_COMM_WORLD);
        }
        local_lines.assign(all_lines.begin() + start, all_lines.begin() + start + count);
    } else {
        int len = 0;
        MPI_Recv(&len, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        char* buffer = new char[len + 1];
        MPI_Recv(buffer, len, MPI_CHAR, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        buffer[len] = '\0';
        string received_data(buffer);
        delete[] buffer;
        local_lines = split_lines(received_data);
    }

    WordCountMap local_map = count_words(local_lines);

    if (rank == 0) {
        WordCountMap global_map = local_map;
        for (int i = 1; i < size; ++i) {
            int recv_size;
            MPI_Recv(&recv_size, 1, MPI_INT, i, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            char* recv_buf = new char[recv_size + 1];
            MPI_Recv(recv_buf, recv_size, MPI_CHAR, i, 3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            recv_buf[recv_size] = '\0';
            istringstream iss(recv_buf);
            string word;
            int count;
            while (iss >> word >> count) {
                global_map[word] += count;
            }
            delete[] recv_buf;
        }

        for (const auto& pair : global_map) {
            cout << pair.first << ": " << pair.second << endl;
        }

        double end_time = MPI_Wtime();
        cout << "Total Execution Time: " << (end_time - start_time) << " seconds" << endl;
    } else {
        ostringstream oss;
        for (const auto& pair : local_map) {
            oss << pair.first << " " << pair.second << "\n";
        }
        string serialized = oss.str();
        int send_len = serialized.length();
        MPI_Send(&send_len, 1, MPI_INT, 0, 2, MPI_COMM_WORLD);
        MPI_Send(serialized.c_str(), send_len, MPI_CHAR, 0, 3, MPI_COMM_WORLD);
    }

    MPI_Finalize();
    return 0;
}