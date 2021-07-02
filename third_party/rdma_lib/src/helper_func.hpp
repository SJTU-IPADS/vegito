#ifndef RDMAIO_HELPER_FUNC_H
#define RDMAIO_HELPER_FUNC_H

// magic number which indicates whether a connection is set
#define TCPSUCC 27
#define TCPFAIL 28

#define MAGIC_NUM 73

inline uint64_t ip_checksum(void* vdata,size_t length) {
  // Cast the data pointer to one that can be indexed.
  char* data = (char*)vdata;

  // Initialise the accumulator.
  uint64_t acc=0xffff;

  // Handle any partial block at the start of the data.
  unsigned int offset=((uintptr_t)data)&3;
  if (offset) {
    size_t count=4-offset;
    if (count>length) count=length;
    uint32_t word=0;
    memcpy(offset+(char*)&word,data,count);
    acc+=ntohl(word);
    data+=count;
    length-=count;
  }

  // Handle any complete 32-bit blocks.
  char* data_end=data+(length&~3);
  while (data!=data_end) {
    uint32_t word;
    memcpy(&word,data,4);
    acc+=ntohl(word);
    data+=4;
  }
  length&=3;

  // Handle any partial block at the end of the data.
  if (length) {
    uint32_t word=0;
    memcpy(&word,data,length);
    acc+=ntohl(word);
  }

  // Handle deferred carries.
  acc=(acc&0xffffffff)+(acc>>32);
  while (acc>>16) {
    acc=(acc&0xffff)+(acc>>16);
  }

  // If the data began at an odd byte address
  // then reverse the byte order to compensate.
  if (offset&1) {
    acc=((acc&0xff00)>>8)|((acc&0x00ff)<<8);
  }

  // Return the checksum in network byte order.
  return htons(~acc);
}

namespace rdmaio {

  struct QPConnArg {
    uint64_t checksum;
    uint64_t qid; // the src qp id
    uint8_t  tid;
    uint8_t  nid;
    uint64_t sign;
    void calculate_checksum() {
      checksum = get_checksum();
    }

    uint64_t get_checksum() {
      return ip_checksum(&(this->qid),sizeof(QPConnArg) - sizeof(uint64_t));
    }
  } __attribute__ ((aligned (CACHE_LINE_SZ)));

};


#endif
