#ifndef COMMON_H
#define COMMON_H

uint64_t startr, endr;
uint64_t startf, endf;
uint64_t startc, endc;
uint64_t startread, endread;

uint64_t start_timer, end_timer;
uint64_t merge_time, fsync_time, computation_fsync_time, file_read_time;

unsigned cycles_low, cycles_high;


static __inline__ unsigned long long rdtsc(void)
{
            __asm__ __volatile__ ("RDTSC\n\t"
                             "mov %%edx, %0\n\t"
                             "mov %%eax, %1\n\t": "=r" (cycles_high), "=r" (cycles_low)::
                             "%rax", "rbx", "rcx", "rdx");
}
#endif
