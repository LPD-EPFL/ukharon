#pragma once

#include <type_traits>

template <typename U>
static constexpr bool isPowerof2(U v) {
    return v && ((v & (v - 1)) == 0);
}

template <typename U>
static typename std::enable_if<std::is_unsigned<U>::value, U>::type roundUp(
    U numToRound, size_t alignment) {
if (!isPowerof2(alignment)) {
    throw std::runtime_error("Provided alignment is not a power of two!");
}
return (numToRound + static_cast<U>(alignment) - 1) &
        -static_cast<U>(alignment);
}

template <typename T>
class CircularBuffer {
public:
    static size_t constexpr Alignment = 64;

    template<size_t Size>
    struct RawBuffer {
        uint8_t dummy[Size];
    };

    CircularBuffer(void *start, size_t len, bool init) {
        auto start_addr = roundUp(reinterpret_cast<uintptr_t>(start), Alignment);
        auto end_addr = reinterpret_cast<uintptr_t>(start) + len;

        if (start_addr >= end_addr) {
            throw std::runtime_error("Cannot allocate CircularBuffer");
        }

        num_elem = (end_addr - start_addr) / sizeof(T);

        if (num_elem < 2) {
            throw std::runtime_error("CircularBuffer should fit at least two elements");
        }

        // Points to first element inside the linear buffer
        this->start = reinterpret_cast<T*>(start_addr);

        // Points to the first element outside of the linear buffer
        this->end = this->start + num_elem;

        if (init) {
            for (size_t i = 0; i < num_elem; i++) {
                this->start[i] = T(this);
            }
        }

        first_new = this->start;
        first_in_process = this->start;
        // free.resize(num_elem, true);
    }

    size_t maxElements() const { return num_elem; }

    static size_t constexpr estimateSize(size_t num) {
        return 2 * Alignment + num * sizeof(T);
    }

    std::optional<T*> firstNew() {
        if (next(first_new) == first_in_process) {
            return std::nullopt;
        }

        return first_new;
    }

    void advanceNew() {
        if (next(first_new) == first_in_process) {
            throw std::runtime_error("Called advanceNew in a full CircularBuffer");
        }

        first_new = next(first_new);
    }

    std::optional<T*> firstInProcess() {
        if (first_new == first_in_process) {
            return std::nullopt;
        }

        return first_in_process;
    }

    void advanceInProcess() {
        if (first_new == first_in_process) {
            throw std::runtime_error("Called advanceInProcess in an empty CircularBuffer");
        }

        first_in_process = next(first_in_process);
    }

    // void markAsFree(T *ptr) {
    //     free[toIdx(ptr)] = true;
    // }

    // void markAsUsed(T *ptr) {
    //     free[toIdx(ptr)] = false;
    // }

    // size_t advanceInProcessConsideringFree() {
    //     size_t processed = 0;

    //     while (auto fip = firstInProcess()) {
    //         if (free[toIdx(fip.value())]) {
    //             advanceInProcess();
    //             processed ++;
    //         } else {
    //             break;
    //         }
    //     }

    //     return processed;
    // }

    inline size_t toIdx(T* ptr) {
        return static_cast<size_t>(ptr - start);
    }

    inline T* fromIdx(size_t idx) {
        return start + idx;
    }

private:
    inline T* next(T* t) {
        t++;
        if (t == end) {
            t = start;
        }

        return t;
    }

    T* start;
    T* end;

    T* first_new;
    T* first_in_process;

    // // Used to for garbage collection. When an element is set
    // // to true, it means that the slot can be reused.
    // // We combine it firstInProgress to make sure that we
    // // reuse slots in order.
    // std::vector<bool> free;

    size_t num_elem;
};