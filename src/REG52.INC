; REG52.INC - Generic 8052 Definitions

P0      DATA    080H
P1      DATA    090H
P2      DATA    0A0H
P3      DATA    0B0H
PSW     DATA    0D0H
ACC     DATA    0E0H
B       DATA    0F0H
SP      DATA    081H
DPL     DATA    082H
DPH     DATA    083H
PCON    DATA    087H
TCON    DATA    088H
TMOD    DATA    089H
TL0     DATA    08AH
TL1     DATA    08BH
TH0     DATA    08CH
TH1     DATA    08DH
IE      DATA    0A8H
IP      DATA    0B8H
SCON    DATA    098H
SBUF    DATA    099H

; TCON
TF1     BIT     TCON.7
TR1     BIT     TCON.6
TF0     BIT     TCON.5
TR0     BIT     TCON.4
IE1     BIT     TCON.3
IT1     BIT     TCON.2
IE0     BIT     TCON.1
IT0     BIT     TCON.0

; IE
EA      BIT     IE.7
ET2     BIT     IE.5
ES      BIT     IE.4
ET1     BIT     IE.3
EX1     BIT     IE.2
ET0     BIT     IE.1
EX0     BIT     IE.0

; SCON
SM0     BIT     SCON.7
SM1     BIT     SCON.6
SM2     BIT     SCON.5
REN     BIT     SCON.4
TB8     BIT     SCON.3
RB8     BIT     SCON.2
TI      BIT     SCON.1
RI      BIT     SCON.0

; P3
RD      BIT     P3.7
WR      BIT     P3.6
T1      BIT     P3.5
T0      BIT     P3.4
INT1    BIT     P3.3
INT0    BIT     P3.2
TXD     BIT     P3.1
RXD     BIT     P3.0
