package server

// globMatch implements Redis-style patterns for KEYS: * ? [abc] [a-z] [^x]
// and \x to escape. It runs in O(len(pattern) * len(s)) worst case, so a
// hostile pattern cannot make KEYS hang.
func globMatch(p, s string) bool {
	pi, si := 0, 0
	starP, starS := -1, 0
	for si < len(s) {
		if pi < len(p) {
			switch p[pi] {
			case '*':
				starP, starS = pi, si
				pi++
				continue
			case '?':
				pi++
				si++
				continue
			case '[':
				matched, next, valid := matchClass(p, pi, s[si])
				if !valid {
					if s[si] == '[' {
						pi++
						si++
						continue
					}
				} else if matched {
					pi = next
					si++
					continue
				}
			case '\\':
				if pi+1 < len(p) && p[pi+1] == s[si] {
					pi += 2
					si++
					continue
				}
			default:
				if p[pi] == s[si] {
					pi++
					si++
					continue
				}
			}
		}
		if starP >= 0 {
			starS++
			si = starS
			pi = starP + 1
			continue
		}
		return false
	}
	for pi < len(p) && p[pi] == '*' {
		pi++
	}
	return pi == len(p)
}

// matchClass tests c against the [...] class starting at p[i].
func matchClass(p string, i int, c byte) (matched bool, next int, valid bool) {
	j := i + 1
	negate := false
	if j < len(p) && p[j] == '^' {
		negate = true
		j++
	}
	start := j
	for j < len(p) {
		if p[j] == ']' && j > start {
			return matched != negate, j + 1, true
		}
		lo := p[j]
		if j+2 < len(p) && p[j+1] == '-' && p[j+2] != ']' {
			hi := p[j+2]
			if lo > hi {
				lo, hi = hi, lo
			}
			if c >= lo && c <= hi {
				matched = true
			}
			j += 3
			continue
		}
		if c == lo {
			matched = true
		}
		j++
	}
	return false, 0, false
}
