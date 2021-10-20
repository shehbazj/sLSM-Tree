#ifndef COMMON_H
#define COMMON_H

uint64_t startr, endr;
uint64_t start_timer, end_timer;
uint64_t merge_time;

unsigned cycles_low, cycles_high;


static __inline__ unsigned long long rdtsc(void)
{
            __asm__ __volatile__ ("RDTSC\n\t"
                             "mov %%edx, %0\n\t"
                             "mov %%eax, %1\n\t": "=r" (cycles_high), "=r" (cycles_low)::
                             "%rax", "rbx", "rcx", "rdx");
}
#endif
