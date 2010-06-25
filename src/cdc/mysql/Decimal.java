// The MIT License
//
// Copyright (c) 2010 Erik Soehnel
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cdc.mysql;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays; // for copyOfRange

public class Decimal {

    // see strings/decimal.c in mysql sources

    public int frac; // the number of *decimal* digits (real 0-9 digits) (NOT number of decimal_digit_t's !) before the point
    public int intg; // number of decimal digits after the point
    public int len;  // the length of buf (length of allocated space) in decimal_digit_t's, not in bytes, initialized with DECIMAL_BUFF_LENGTH???
    public boolean sign; 
    public int[] buf; // buf is an array of decimal_digit_t's (decimal_digit_t refers to an int, decimals are stored to base 10⁹)
    
    // CONSTANTS
    public static final int DIG_PER_DEC1 = 9;
    public static final int DIG_MASK     = 100000000;
    public static final int DIG_BASE     = 1000000000; // the range of one decimal_digit_t
    public static final int DIG_MAX      = DIG_BASE-1;
    public static final long DIG_BASE2   = DIG_BASE * DIG_BASE;
    public static final byte[] dig2bytes = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
    public static final int DECIMAL_BUFF_LENGTH = 9; // 9 decimal_digit_t's
    public static final int[] powers10={1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

    public static final int sizeof_dec1 = 4; // an int32

    public static final int E_DEC_OK              =  0;
    public static final int E_DEC_TRUNCATED       =  1;
    public static final int E_DEC_OVERFLOW        =  2;
    public static final int E_DEC_DIV_ZERO        =  4;
    public static final int E_DEC_BAD_NUM         =  8;
    public static final int E_DEC_OOM             = 16;    
    public static final int E_DEC_ERROR           = 31;
    public static final int E_DEC_FATAL_ERROR     = 30;

    // default ctor
    public Decimal() {
        this.buf = new int[DECIMAL_BUFF_LENGTH];
        this.len = DECIMAL_BUFF_LENGTH;
    }

    static void throwf(String msg) {
        throw new RuntimeException(msg);
    }

    // helpers for integer packing, see myisampack.h
    // "Storing of values in high byte first order." (for better compression)
    // the original macros expect an unsigned byte-array

    public static int ubyte(byte a) {
        // returns the bytes value as an int as if it were an unsigned byte
        return a & ((int) 0xff);
    }

    public static int mi_sint1korr(byte[] a) {
        return ubyte(a[0]);
    }

    public static int mi_sint2korr(byte[] a) {
        return ((int) (ubyte(a[1]) + (ubyte(a[0]) << 8))); // was originally an int16
    }

    public static int mi_sint3korr(byte[] a) {
        return ((a[0] < 0)   // looks for bit 0x80 (the first bit in a -> signed or unsigned)
                ? ( (((int)  255) << 24) // when signed, fills the MSB of our int with 0xff to correctly represent two's-complement of the remaing 3 byte value
                   |(((int) ubyte(a[0])) << 16)
                   |(((int) ubyte(a[1])) << 8)
                   | ((int) ubyte(a[2])))
                : ( (((int) ubyte(a[0])) << 16)
                   |(((int) ubyte(a[1])) << 8)
                   | ((int) ubyte(a[2]))));
    }

    public static int mi_sint4korr(byte [] a) {
        return ( ((int) ubyte(a[3])) +
                (((int) ubyte(a[2])) << 8) +
                (((int) ubyte(a[1])) << 16) +
                (((int) ubyte(a[0])) << 24));
    }

    public static int roundUp(int x) {
        //orig: #define ROUND_UP(X)  (((X)+DIG_PER_DEC1-1)/DIG_PER_DEC1)
        return (x+DIG_PER_DEC1-1)/DIG_PER_DEC1;
    }

    // methods on and for Decimals

    // Returns the size of array to hold a binary representation of a decimal
    public static int decimalBinSize(int precision, int scale)
    {
        int intg=precision-scale,
            intg0=intg/DIG_PER_DEC1, frac0=scale/DIG_PER_DEC1,
            intg0x=intg-intg0*DIG_PER_DEC1, frac0x=scale-frac0*DIG_PER_DEC1;
        
        //DBUG_ASSERT(scale >= 0 && precision > 0 && scale <= precision);
        return intg0*sizeof_dec1+dig2bytes[intg0x]+
               frac0*sizeof_dec1+dig2bytes[frac0x];
    }
    
    // Restores decimal from its binary fixed-length representation
    // modifies its src argument!!!
    //orig: int bin2decimal(const uchar *from, decimal_t *to, int precision, int scale)
    public static Decimal binToDecimal(byte[] from, int precision, int scale)
    {
        int error=E_DEC_OK, intg=precision-scale,
            intg0=intg/DIG_PER_DEC1, frac0=scale/DIG_PER_DEC1,
            intg0x=intg-intg0*DIG_PER_DEC1, frac0x=scale-frac0*DIG_PER_DEC1,
            intg1=intg0+((intg0x>0)?1:0), frac1=frac0+((frac0x>0)?1:0);
        
        //new:
        Decimal to = new Decimal();
        int[] buf = to.buf;
        int bI = 0; // index into to.buf 
        //orig: dec1 *buf=to->buf, mask=(*from & 0x80) ? 0 : -1;
        int mask=((from[0] & 0x80)!=0) ? 0 : -1;
        
        //orig: const uchar *stop;
        //orig: uchar *d_copy;
        //orig: int bin_size= decimal_bin_size(precision, scale);
        int bin_size= decimalBinSize(precision, scale);

        //orig: sanity(to);
        //orig: d_copy= (uchar*) my_alloca(bin_size);
        //orig: memcpy(d_copy, from, bin_size);
        //orig: d_copy[0]^= 0x80;
        //orig: from= d_copy;
        ////ByteBuffer from = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
        ////from.put(from.get(0)^0x80);
        from[0] ^= 0x80;
        int fI = 0; // index into from, for c-style pointer arithmetic

        //orig: FIX_INTG_FRAC_ERROR(to->len, intg1, frac1, error);
        //expanded macro: FIX_INTG_FRAC_ERROR(len, intg1, frac1, error)
        if (intg1+frac1 > to.len) {
            if (intg1 > to.len) {
                intg1=to.len;
                frac1=0;
                error=E_DEC_OVERFLOW;
            } else {
                frac1=to.len-intg1;
                error=E_DEC_TRUNCATED;
            }
        } else {
            error=E_DEC_OK;
        }
        // FIX_INTG_FRAC_ERROR macro end

        if (error!=0) {
            if (intg1 < intg0+((intg0x>0)?1:0)) {
                //orig: from+=dig2bytes[intg0x]+sizeof(dec1)*(intg0-intg1);
                fI = dig2bytes[intg0x]+sizeof_dec1*(intg0-intg1);
                frac0=frac0x=intg0x=0;
                intg0=intg1;
            } else {
                frac0x=0;
                frac0=frac1;
            }
        }

        to.sign=(mask != 0);
        to.intg=intg0*DIG_PER_DEC1+intg0x;
        to.frac=frac0*DIG_PER_DEC1+frac0x;

        if (intg0x != 0) {
            int i=dig2bytes[intg0x];
            //orig: dec1 UNINIT_VAR(x); // supposedly to avoid warnings from uninitialized vars
            int x = 0; // initialize with 0 to supress warnings
            byte fromA[] = Arrays.copyOfRange(from,fI, fI+i); // "read" i bytes
            switch (i) { // de-pack bytes into (signed)ints (those bytes are in big-endian (MSB at lowest address))
            case 1: x=mi_sint1korr(fromA); break;
            case 2: x=mi_sint2korr(fromA); break;
            case 3: x=mi_sint3korr(fromA); break;
            case 4: x=mi_sint4korr(fromA); break;
            default: throwf("This must not happen.");
            }
            //orig: from+=i;
            fI += i;
            //orig: *buf=x ^ mask;
            buf[bI] = x ^ mask;
            if (buf[bI] >= powers10[intg0x+1]) throwf("E_DEC_BAD_NUM");
            //orig: if (buf > to->buf || *buf != 0) // (buf > to->buf) checks wether buf has been incremented
            if (bI > 0 || buf[bI] != 0)
                //orig: buf++
                bI++;
            else
                to.intg-=intg0x;
        }
        //orig: for (stop=from+intg0*sizeof(dec1); from < stop; from+=sizeof(dec1))
        for (int stop=fI+intg0*sizeof_dec1; fI < stop; fI += sizeof_dec1)
            {
                // we need an index over the buf array and a method-global counter here!!!!
                //DBUG_ASSERT(sizeof(dec1) == 4);
                //orig: *buf=mi_sint4korr(from) ^ mask;
                byte[] fromA = Arrays.copyOfRange(from, fI, fI+4);
                buf[bI] = mi_sint4korr(fromA) ^ mask;
                //orig: if (((uint32)*buf) > DIG_MAX) goto err;
                if (buf[bI] > DIG_MAX) throwf("E_DEC_BAD_NUM");
                //orig: if (buf > to->buf || *buf != 0)
                if (bI > 0 || buf[bI] != 0)
                    bI++;
                else
                    to.intg-=DIG_PER_DEC1;
            }
        //DBUG_ASSERT(to->intg >=0);
        for (int stop=fI+frac0*sizeof_dec1; fI < stop; fI += sizeof_dec1)
            {
                //DBUG_ASSERT(sizeof(dec1) == 4);
                byte[] fromA = Arrays.copyOfRange(from, fI, fI+4);
                buf[bI]=mi_sint4korr(fromA) ^ mask;
                if (buf[bI] > DIG_MAX) throwf("E_DEC_BAD_NUM");
                bI++;
            }
        if (frac0x != 0)
            {
                int i=dig2bytes[frac0x];
                //dec1 UNINIT_VAR(x);
                int x = 0; // initialize with 0 to supress warnings
                byte[] fromA = Arrays.copyOfRange(from, fI, fI+i);
                switch (i)
                    {
                    case 1: x=mi_sint1korr(fromA); break;
                    case 2: x=mi_sint2korr(fromA); break;
                    case 3: x=mi_sint3korr(fromA); break;
                    case 4: x=mi_sint4korr(fromA); break;
                    default: throwf("this must not happen");
                    }
                buf[bI]=(x ^ mask) * powers10[DIG_PER_DEC1 - frac0x];
                if (buf[bI] > DIG_MAX) throwf("E_DEC_BAD_NUM");
                bI++;
            }
        //my_afree(d_copy);
        return to;

        // err:
        // my_afree(d_copy);
        // decimal_make_zero(((decimal_t*) to));
        // return(E_DEC_BAD_NUM);
    }

    public String toString() {
      int i, end;
      StringBuffer sb = new StringBuffer();
      //pos+= my_sprintf(buff, (buff, "%s", dec.sign() ? "-" : ""));
      sb.append(this.sign?"-":"");
      
      end= roundUp(this.frac) + roundUp(this.intg)-1;
      for (i=0; i < end; i++) {
          //pos+= my_sprintf(pos, (pos, "%09d.", dec.buf[i]));
          sb.append(String.format("%09d.", this.buf[i]));
      }
      //pos+= my_sprintf(pos, (pos, "%09d", dec.buf[i]));
      sb.append(String.format("%09d", this.buf[i]));

      return sb.toString();
    }
}

