#include <mpi.h>
#include <pthread.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <map>
#include <set>
#include <algorithm>

#define TRACKER_RANK 0
#define MAX_FILES 10
#define MAX_FILENAME 15
#define HASH_SIZE 33
#define MAX_CHUNKS 100

#define MAX_SEEDS 10
#define MAX_PEERS 10

// structura folosita atunci cand un client
// cere informatii despre un fisier
struct SwarmResponse {
    int file_hashes_count;
    char file_hashes[MAX_CHUNKS][HASH_SIZE];
    int seed_count;
    int seeds[MAX_SEEDS];
    int peer_count;
    int peers[MAX_PEERS];
};

// structura unui fisier
struct File {
    char name[MAX_FILENAME];
    int segment_count;
    char segments[MAX_CHUNKS][HASH_SIZE];
};

// structura transmisa de un client catre tracker
struct ClientData {
    int rank;
    int num_owned_files;
    File owned_files[MAX_FILES];
};

// structura pentru starea unui client
struct ClientState {
    ClientData client_data;
    std::vector<std::string> desired_files;
    std::vector<std::string> owned_hashes;
    pthread_mutex_t data_mutex;
};

// functia pentru citirea din fisierul de intrare
// si stocarea datelor clientului
void read_input_file(int rank, ClientData& client_data, std::vector<std::string>& desired_files) {
    // se deschide fisierul
    std::ifstream infile("in" + std::to_string(rank) + ".txt");
    if (!infile) {
        std::cout << "Eroare la deschiderea fisierului" << std::endl;
        std::exit(-1);
    }

    client_data.rank = rank;

    // se citesc numele fisierelor detinute de client
    // si hash-urile segmentelor acestora
    infile >> client_data.num_owned_files;
    for (int i = 0; i < client_data.num_owned_files; i++) {
        infile >> client_data.owned_files[i].name >> client_data.owned_files[i].segment_count;
        for (int j = 0; j < client_data.owned_files[i].segment_count; j++) {
            infile >> client_data.owned_files[i].segments[j];
        }
    }

    // se citesc numele fisierelor dorite de client
    int num_desired_files;
    infile >> num_desired_files;
    desired_files.resize(num_desired_files);
    for (int i = 0; i < num_desired_files; i++) {
        infile >> desired_files[i];
    }
}

// functie pentru crearea tipului MPI pentru SwarmResponse
MPI_Datatype create_swarm_response_type() {
    const int num_items = 6;
    int blockcounts[6] = {
        1,                           
        MAX_CHUNKS * HASH_SIZE,
        1,
        MAX_SEEDS,
        1,
        MAX_PEERS
    };
    MPI_Datatype types[6] = {
        MPI_INT,
        MPI_CHAR,
        MPI_INT,
        MPI_INT,
        MPI_INT,
        MPI_INT
    };
    MPI_Aint offsets[6];
    offsets[0] = offsetof(SwarmResponse, file_hashes_count);
    offsets[1] = offsetof(SwarmResponse, file_hashes);
    offsets[2] = offsetof(SwarmResponse, seed_count);
    offsets[3] = offsetof(SwarmResponse, seeds);
    offsets[4] = offsetof(SwarmResponse, peer_count);
    offsets[5] = offsetof(SwarmResponse, peers);

    MPI_Datatype swarm_response_type;
    MPI_Type_create_struct(num_items, blockcounts, offsets, types, &swarm_response_type);
    MPI_Type_commit(&swarm_response_type);

    return swarm_response_type;
}

// functie pentru crearea tipului MPI pentru ClientData
MPI_Datatype create_client_data_type() {
    // definirea tipului MPI pentru fisier
    MPI_Datatype file_type;
    MPI_Datatype oldtypes_file[3] = {MPI_CHAR, MPI_INT, MPI_CHAR};
    int blockcounts_file[3] = {MAX_FILENAME, 1, MAX_CHUNKS * HASH_SIZE};
    MPI_Aint offsets_file[3];
    offsets_file[0] = offsetof(File, name);
    offsets_file[1] = offsetof(File, segment_count);
    offsets_file[2] = offsetof(File, segments);

    MPI_Type_create_struct(3, blockcounts_file, offsets_file, oldtypes_file, &file_type);
    MPI_Type_commit(&file_type);


    // definirea tipului MPI pentru ClientData
    MPI_Datatype client_data_type;
    MPI_Datatype oldtypes_client[3] = {MPI_INT, MPI_INT, file_type};
    int blockcounts_client[3] = {1, 1, MAX_FILES};
    MPI_Aint offsets_client[3];
    offsets_client[0] = offsetof(ClientData, rank);
    offsets_client[1] = offsetof(ClientData, num_owned_files);
    offsets_client[2] = offsetof(ClientData, owned_files);

    MPI_Type_create_struct(3, blockcounts_client, offsets_client, oldtypes_client, &client_data_type);
    MPI_Type_commit(&client_data_type);
    

    MPI_Type_free(&file_type);

    return client_data_type;
}

void tracker(int numtasks, int rank) {
    // vector care retine toate informatiile despre clienti
    std::vector<ClientData> client_info;

    // map care retine hash-urile segmentelor pentru fiecare fisier
    std::map<std::string, std::vector<std::string>> file_hashes;

    // map care retine pentru fiecare fisier care sunt seed-urile acestuia
    std::map<std::string, std::set<int>> file_seeds;

    // map care retine pentru fiecare fisier care sunt peer-urile acestuia
    std::map<std::string, std::set<int>> file_peers;

    // set care retine clientii care au terminat
    std::set<int> finished_clients;

    // creearea tipurilor pentru SwarmResponse si ClientData
    MPI_Datatype swarm_response_type = create_swarm_response_type();
    MPI_Datatype client_data_type = create_client_data_type();

    // primesc de la fiecare client informatiile despre acesta
    for (int i = 1; i < numtasks; i++) {
        ClientData client_data;
        MPI_Status status;
        MPI_Recv(&client_data, 1, client_data_type, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);

        // retin datele clientului
        client_info.push_back(client_data);

        // populez map-urile care retin corespondenta intre fisier si hash-uri
        // si intre fisier si seed-uri
        for (int j = 0; j < client_data.num_owned_files; j++) {
            std::string file_name(client_data.owned_files[j].name);

            // daca exista deja fisierul in map, trec la urmatorul
            if (file_hashes.find(file_name) != file_hashes.end()) {
                continue;
            }

            for (int k = 0; k < client_data.owned_files[j].segment_count; k++) {
                std::string hash(client_data.owned_files[j].segments[k]);
                file_hashes[file_name].push_back(hash);
            }
            file_seeds[file_name].insert(client_data.rank);
        }
    }

    // trimit mesajul de "READY" catre toti clientii
    // pentru a-i anunta ca pot incepe
    for (int i = 1; i < numtasks; i++) {
        const char* ready_message = "READY";
        MPI_Send(ready_message, strlen(ready_message) + 1, MPI_CHAR, i, 0, MPI_COMM_WORLD);
    }

    // cat timp nu s-au terminat toti clientii
    bool all_done = false;
    while (!all_done) {
        char recv_buffer[MAX_CHUNKS];
        MPI_Status recv_status;
        MPI_Recv(recv_buffer, MAX_CHUNKS, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &recv_status);

        int sender = recv_status.MPI_SOURCE;
        std::string message(recv_buffer);

        // daca este mesaj de tip "swarm:<file_name>"
        if (message.find("swarm:") == 0) {
            // extrag numele fisierului
            std::string file_name = message.substr(6);

            SwarmResponse sr;

            // retin hash-urile pentru a le trimite clientului
            sr.file_hashes_count = (int) file_hashes[file_name].size();
            for (int i = 0; i < sr.file_hashes_count; i++) {
                strncpy(sr.file_hashes[i], file_hashes[file_name][i].c_str(), HASH_SIZE - 1);
                sr.file_hashes[i][HASH_SIZE - 1] = '\0';
            }

            // retin seed-urile pentru a le trimite clientului
            sr.seed_count = (int) file_seeds[file_name].size();
            int i = 0;
            for (const auto& seed : file_seeds[file_name]) {
                sr.seeds[i++] = seed;
            }

            // retin peer-urile pentru a-i trimite clientului
            sr.peer_count = (int) file_peers[file_name].size();
            i = 0;
            for (const auto& peer : file_peers[file_name]) {
                sr.peers[i++] = peer;
            }

            // trimit SwarmResponse catre client
            MPI_Send(&sr, 1, swarm_response_type, sender, 0, MPI_COMM_WORLD);

            // adaug peer-ul care a cerut informatiile despre fisier
            file_peers[file_name].insert(sender);
        }
        // daca este mesaj de tip "finished_file:<file_name>"
        else if (message.find("finished_file:") == 0) {
            // extrag numele fisierului
            std::string file_name = message.substr(14);

            // sterg peer-ul care a terminat de descarcat fisierul
            // si il adaug in lista de seed-uri
            file_peers[file_name].erase(sender);
            file_seeds[file_name].insert(sender);
        }
        // daca este mesaj de tip "total_finished"
        else if (message.find("total_finished") == 0) {
            // adaug clientul care a terminat
            finished_clients.insert(sender);

            // verific daca toti clientii au terminat
            // daca da, trimit mesajul "done" catre toti clientii
            // astfel incat acestia sa se opreasca
            // si opresc bucla tracker-ului
            if ((int)finished_clients.size() == (numtasks - 1)) {
                for (int i = 1; i < numtasks; i++) {
                    const char* done_message = "done";
                    MPI_Send(done_message, strlen(done_message) + 1, MPI_CHAR, i, 1, MPI_COMM_WORLD);
                }
                all_done = true;
            }
        }
        // daca este mesaj de tip "update_swarm:<file_name>"
        else if (message.find("update_swarm:") == 0) {
            // extrag numele fisierului
            std::string file_name = message.substr(13);

            // creez un nou SwarmResponse updatat pentru a trimite informatiile actualizate
            SwarmResponse sr;

            sr.file_hashes_count = (int) file_hashes[file_name].size();
            for (int i = 0; i < sr.file_hashes_count; i++) {
                strncpy(sr.file_hashes[i], file_hashes[file_name][i].c_str(), HASH_SIZE - 1);
                sr.file_hashes[i][HASH_SIZE - 1] = '\0';
            }

            sr.seed_count = (int) file_seeds[file_name].size();
            int i = 0;
            for (const auto& seed : file_seeds[file_name]) {
                sr.seeds[i++] = seed;
            }

            sr.peer_count = (int) file_peers[file_name].size();
            i = 0;
            for (const auto& peer : file_peers[file_name]) {
                sr.peers[i++] = peer;
            }

            // trimit SwarmResponse catre client
            MPI_Send(&sr, 1, swarm_response_type, sender, 0, MPI_COMM_WORLD);
        }
        else {
            std::cout << "Eroare" << std::endl;
        }
    }
}

// functie pentru thread-ul de upload
void* upload_thread_func(void* arg) {
    ClientState* state = (ClientState*)arg;

    // cat timp inca exista clienti care nu au terminat
    while (true) {
        // asttept mesaj de la un client
        char recv_buffer[MAX_CHUNKS];
        MPI_Status recv_status;
        MPI_Recv(recv_buffer, MAX_CHUNKS, MPI_CHAR, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &recv_status);

        int requester = recv_status.MPI_SOURCE;
        std::string request(recv_buffer);

        // daca este mesaj de tip "request_segment:<hash>"
        if (request.find("request_segment:") == 0) {
            // extrag hash-ul segmentului
            std::string requested_hash = request.substr(16);

            // verific daca clientul detine segmentul
            bool has_hash = false;

            pthread_mutex_lock(&state->data_mutex);
            for (const auto& hash : state->owned_hashes) {
                if (hash == requested_hash) {
                    has_hash = true;
                    break;
                }
            }
            pthread_mutex_unlock(&state->data_mutex);

            // daca clientul detine segmentul, trimit mesajul "OK"
            // si hash-ul se poate descarca
            // altfel trimit mesajul "Negative"
            if (has_hash) {
                std::string ok_response = "OK";
                MPI_Send(ok_response.c_str(), ok_response.size() + 1, MPI_CHAR, requester, 2, MPI_COMM_WORLD);
            }
            else {
                std::string neg_response = "Negative";
                MPI_Send(neg_response.c_str(), neg_response.size() + 1, MPI_CHAR, requester, 2, MPI_COMM_WORLD);
            }
        }
        // daca este mesaj de tip "done"
        // clientul a terminat si thread-ul se opreste
        else if (request.find("done") == 0) {
            break;
        }
        else {
            std::cout << "Eroare" << std::endl;
        }
    }

    pthread_exit(nullptr);
}

// functie pentru thread-ul de download
void* download_thread_func(void* arg) {
    ClientState* state = (ClientState*)arg;
    int rank = state->client_data.rank;

    // vector care retine hash-urile segmentelor descarcate
    std::vector<std::string> downloaded_hashes;

    // creez tipul MPI pentru SwarmResponse
    MPI_Datatype swarm_response_type = create_swarm_response_type();

    // pentru fiecare fisier dorit de client
    for (const auto& file_name : state->desired_files) {
        // trimit mesaj catre tracker pentru a cere informatii despre fisier
        std::string swarm_request = "swarm:" + file_name;
        MPI_Send(swarm_request.c_str(), swarm_request.size() + 1, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);

        // astept SwarmResponse de la tracker
        SwarmResponse sr;
        MPI_Status recv_status;
        MPI_Recv(&sr, 1, swarm_response_type, TRACKER_RANK, 0, MPI_COMM_WORLD, &recv_status);

        int downloaded_segments = 0;
        for (int i = 0; i < sr.file_hashes_count; i++) {
            std::string hash(sr.file_hashes[i]);

            // combin si amestec aleatoriu seed-urile si peer-urile
            std::vector<int> combined_sources(sr.seeds, sr.seeds + sr.seed_count);
            combined_sources.insert(combined_sources.end(), sr.peers, sr.peers + sr.peer_count);
            std::random_shuffle(combined_sources.begin(), combined_sources.end());

            // pentru fiecare seed sau peer
            for (int source : combined_sources) {
                if (source == rank) continue;

                // trimit cerere de segment
                std::string segment_request = "request_segment:" + hash;
                MPI_Send(segment_request.c_str(), segment_request.size() + 1, MPI_CHAR, source, 1, MPI_COMM_WORLD);

                // astept raspuns de la seed sau peer
                char response_buffer[MAX_CHUNKS];
                MPI_Status response_status;
                MPI_Recv(response_buffer, MAX_CHUNKS, MPI_CHAR, source, 2, MPI_COMM_WORLD, &response_status);
                std::string segment_response(response_buffer);

                // daca seed-ul sau peer-ul detine segmentul, il descarc
                if (segment_response.find("OK") == 0) {
                    pthread_mutex_lock(&state->data_mutex);
                    state->owned_hashes.push_back(hash);
                    downloaded_hashes.push_back(hash);
                    pthread_mutex_unlock(&state->data_mutex);

                    downloaded_segments++;
                    break;
                }
            }

            // la fiecare 10 segmente descarcate, trimit un mesaj catre tracker
            if (downloaded_segments % 10 == 0 && downloaded_segments != 0) {
                std::string swarm_update_request = "swarm:" + file_name;
                MPI_Send(swarm_update_request.c_str(), swarm_update_request.size() + 1, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);

                MPI_Recv(&sr, 1, swarm_response_type, TRACKER_RANK, 0, MPI_COMM_WORLD, &recv_status);
            }

        }

        // dupa ce am terminat de descarcat un fisier,
        // trimit mesaj catre tracker ca am terminat
        std::string finished_file_message = "finished_file:" + file_name;
        MPI_Send(finished_file_message.c_str(), finished_file_message.size() + 1, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);

        // scriu hash-urile descarcate in fisier
        std::ofstream outfile("client" + std::to_string(rank) + "_" + file_name);
        if (!outfile) {
            std::cout << "Eroare la deschiderea fisierului" << std::endl;
        }
        else {
            for (int i = 0; i < (int)downloaded_hashes.size(); i++) {
                outfile << downloaded_hashes[i];
                if (i != (int)downloaded_hashes.size() - 1) {
                    outfile << "\n";
                }
            }
            outfile.close();
            // eliberez vectorul de hash-uri descarcate
            downloaded_hashes.clear();
        }
    }

    // cand termin de descarcat toate fisierele dorite
    // trimit mesaj catre tracker ca am terminat tot de descarcat
    std::string total_finished_message = "total_finished";
    MPI_Send(total_finished_message.c_str(), total_finished_message.size() + 1, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);

    MPI_Type_free(&swarm_response_type);

    pthread_exit(nullptr);
}

// functie pentru client
void peer(int numtasks, int rank) {
    ClientState state;

    // initializez mutex-ul
    pthread_mutex_init(&state.data_mutex, nullptr);

    // citesc datele clientului din fisier
    read_input_file(rank, state.client_data, state.desired_files);

    // retin hash-urile segmentelor detinute de client
    for (int i = 0; i < state.client_data.num_owned_files; i++) {
        for (int j = 0; j < state.client_data.owned_files[i].segment_count; j++) {
            std::string hash(state.client_data.owned_files[i].segments[j]);
            state.owned_hashes.push_back(hash);
        }
    }
    
    // creez tipurile MPI pentru SwarmResponse si ClientData
    MPI_Datatype swarm_response_type = create_swarm_response_type();
    MPI_Datatype client_data_type = create_client_data_type();

    // trimit datele initiale ale clientului catre tracker
    MPI_Send(&state.client_data, 1, client_data_type, TRACKER_RANK, 0, MPI_COMM_WORLD);

    MPI_Type_free(&client_data_type);

    // astept mesaj de la tracker pentru a incepe
    char ready_message[MAX_CHUNKS];
    MPI_Status status_recv;
    MPI_Recv(ready_message, MAX_CHUNKS, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD, &status_recv);


    pthread_t download_thread;
    pthread_t upload_thread;
    int rc;

    rc = pthread_create(&download_thread, nullptr, download_thread_func, (void*)&state);
    if (rc) {
        std::cerr << "Eroare la crearea thread-ului de download" << std::endl;
        std::exit(-1);
    }

    rc = pthread_create(&upload_thread, nullptr, upload_thread_func, (void*)&state);
    if (rc) {
        std::cerr << "Eroare la crearea thread-ului de upload" << std::endl;
        std::exit(-1);
    }

    rc = pthread_join(download_thread, nullptr);
    if (rc) {
        std::cerr << "Eroare la asteptarea thread-ului de download" << std::endl;
        std::exit(-1);
    }

    rc = pthread_join(upload_thread, nullptr);
    if (rc) {
        std::cerr << "Eroare la asteptarea thread-ului de upload" << std::endl;
        std::exit(-1);
    }

    MPI_Type_free(&swarm_response_type);

    pthread_mutex_destroy(&state.data_mutex);
}

int main(int argc, char* argv[]) {
    int numtasks, rank;

    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        std::cerr << "MPI nu are suport pentru multi-threading" << std::endl;
        std::exit(-1);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == TRACKER_RANK) {
        tracker(numtasks, rank);
    }
    else {
        peer(numtasks, rank);
    }

    MPI_Finalize();
    return 0;
}
