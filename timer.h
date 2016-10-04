#ifndef TIMER_H
#define TIMER_H
#include <ctime>

class Timer {
    std::clock_t start_time;

public:
    /**
     * Start the timer
     */
    void start();
    
    /**
     * @return the elapsed time, in seconds
     */
    double getElapsedTime();
};

void Timer::start() {
    start_time = std::clock();
}

double Timer::getElapsedTime() {
    return ( std::clock() - start_time ) / (double) CLOCKS_PER_SEC;
}

#endif //TIMER_H