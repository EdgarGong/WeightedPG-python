import numpy as np
import warnings
warnings.filterwarnings('ignore', 'overflow encountered in uint_scalars')

crush_hash_seed = 1315423911

def crush_hashmix(a, b, c):
	a = np.uint32(a)
	b = np.uint32(b)
	c = np.uint32(c)

	a = np.uint32(a-b);  a = np.uint32(a-c);  a = np.uint32(a^(c>>13))
	b = np.uint32(b-c);  b = np.uint32(b-a);  b = np.uint32(b^(a<<8))
	c = np.uint32(c-a);  c = np.uint32(c-b);  c = np.uint32(c^(b>>13))
	a = np.uint32(a-b);  a = np.uint32(a-c);  a = np.uint32(a^(c>>12))
	b = np.uint32(b-c);  b = np.uint32(b-a);  b = np.uint32(b^(a<<16))
	c = np.uint32(c-a);  c = np.uint32(c-b);  c = np.uint32(c^(b>>5))
	a = np.uint32(a-b);  a = np.uint32(a-c);  a = np.uint32(a^(c>>3))
	b = np.uint32(b-c);  b = np.uint32(b-a);  b = np.uint32(b^(a<<10))
	c = np.uint32(c-a);  c = np.uint32(c-b);  c = np.uint32(c^(b>>15))
	return a, b, c
	

def crush_hash32_rjenkins1_2(a, b):
	a = np.uint32(a)
	b = np.uint32(b)

	hash = np.uint32(crush_hash_seed ^ a ^ b)
	x = np.uint32(231232)
	y = np.uint32(1232)
	a, b, hash = crush_hashmix(a, b, hash)
	x, a, hash = crush_hashmix(x, a, hash)
	b, y, hash = crush_hashmix(b, y, hash)
	return np.uint32(hash)


def ceph_stable_mod(x, b, bmask):
	x = np.uint32(x)
	b = np.uint32(b)
	bmask = np.uint32(bmask)

	if np.uint32(x & bmask) < np.uint32(b):
		return np.uint32(x & bmask)
	else:
		return np.uint32(x & (bmask >> 1))
	

def mix(a, b, c):
	a = np.uint32(a)
	b = np.uint32(b)
	c = np.uint32(c)

	a = np.uint32(a - b);  a = np.uint32(a - c);  a = np.uint32(a ^ (c >> 13))
	b = np.uint32(b - c);  b = np.uint32(b - a);  b = np.uint32(b ^ (a << 8))
	c = np.uint32(c - a);  c = np.uint32(c - b);  c = np.uint32(c ^ (b >> 13))
	a = np.uint32(a - b);  a = np.uint32(a - c);  a = np.uint32(a ^ (c >> 12))
	b = np.uint32(b - c);  b = np.uint32(b - a);  b = np.uint32(b ^ (a << 16))
	c = np.uint32(c - a);  c = np.uint32(c - b);  c = np.uint32(c ^ (b >> 5))
	a = np.uint32(a - b);  a = np.uint32(a - c);  a = np.uint32(a ^ (c >> 3))
	b = np.uint32(b - c);  b = np.uint32(b - a);  b = np.uint32(b ^ (a << 10))
	c = np.uint32(c - a);  c = np.uint32(c - b);  c = np.uint32(c ^ (b >> 15))


	return a, b, c


def ceph_str_hash_rjenkins(k, length):
	len = np.uint32(length)
	a = np.uint32(0x9e3779b9)
	b = np.uint32(a)
	c = np.uint32(0)


	while len >= 12:
		a = np.uint32(a + (ord(k[0]) + (ord(k[1]) << 8) + (ord(k[2]) << 16) +
			 (ord(k[3]) << 24)))
		b = np.uint32(b + (ord(k[4]) + (ord(k[5]) << 8) + (ord(k[6]) << 16) +
			 (ord(k[7]) << 24)))
		c = np.uint32(c + (ord(k[8]) + (ord(k[9]) << 8) + (ord(k[10]) << 16) +
			 (ord(k[11]) << 24)))


		a, b, c = mix(a, b, c)

		k = k[12:]
		len = len - 12

	c = np.uint32(c + length)

	if len >= 1 and len <= 11:
		if len == 11:
			c = np.uint32(c + (ord(k[10]) << 24))
		if len >= 10:
			c = np.uint32(c + (ord(k[9]) << 16))
		if len >= 9:
			c = np.uint32(c + (ord(k[8]) << 8))
		if len >= 8:
			b = np.uint32(b + (ord(k[7]) << 24))
		if len >= 7:
			b = np.uint32(b + (ord(k[6]) << 16))
		if len >= 6:
			b = np.uint32(b + (ord(k[5]) << 8))
		if len >= 5:
			b = np.uint32(b + ord(k[4]))
		if len >= 4:
			a = np.uint32(a + (ord(k[3]) << 24))
		if len >= 3:
			a = np.uint32(a + (ord(k[2]) << 16))
		if len >= 2:
			a = np.uint32(a + (ord(k[1]) << 8))
		if len >= 1:
			a = np.uint32(a + ord(k[0]))
		
	a, b, c = mix(a, b, c)

	return c
	
