
#include <vector>
#include <fstream>
#include <iostream>

#include "parallel_for.h"

//using namespace profane;
//
//struct PLogTraits
//{
//    using Clock = std::chrono::high_resolution_clock;
//
//#pragma pack(push)
//#pragma pack(1)
//    struct EventData
//    {
//        int threadId;
//    };
//#pragma pack(pop)
//
//    static void OnWorkItem(const EventData& eventData, WorkItemProto<Clock>& workItemProto)
//    {
//        workItemProto.workerName = std::to_string(eventData.threadId);
//        workItemProto.routineName = "W";
//    }
//};
//
//using PLog = PerfLogger<PLogTraits>;
//PLog plog{};

struct Matrix
{
    int height;
    int width;
    std::vector<double> data;

    double at(int y, int x) const { return data[width * y + x]; }
    double& at(int y, int x) { return data[width * y + x]; }
};

Matrix GenerateMatrix(int height, int width)
{
    Matrix mat { height, width };
    mat.data.resize(height * width);
    int idx = 0;
    for (auto& value : mat.data)
        value = static_cast<double>(idx++ % 5) - 2.0;
    return mat;
}

Matrix MatMul(ThreadHive& hive, const Matrix& A, const Matrix& B)
{
    assert(A.width == B.height);
    Matrix X;
    X.height = A.height;
    X.width = B.width;
    X.data.resize(X.height * X.width, 0.0);

    hive.For(0, X.height, 1, 16, [&](int y0, int y1) {
        //auto ttx = plog.Trace((int)std::hash<std::thread::id>{}(std::this_thread::get_id()));
        while (y0 < y1) {
            int y = y0++;
            //hive.For(0, X.width, [&, y](int x0, int x1) {
            for (int x = 0; x < X.width; ++x)
            {
                //while (x0 < x1) {
                    //int x = x0++;
                    for (int k = 0; k < A.width; ++k)
                    {
                        X.at(y, x) += A.at(y, k) * B.at(k, x);
                    }
                //}
            }//);
        }
    });

    return X;
}

int main()
{
    //plog.Enable("perflog.bin", 100000000);

    auto A = GenerateMatrix(340, 450);
    auto B = GenerateMatrix(450, 760);

    ThreadHive hive;

    auto X = MatMul(hive, A, B);
}
