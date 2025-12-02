; ================================================================
; Файл: isr_uart.asm
; Описание: Прерывание UART и выполнение команд
; ================================================================

$INCLUDE (defs.inc)

PUBLIC UART_ISR, Process_Command
EXTRN DATA (RX_BUFFER, RX_INDEX, FLAGS)

CSEG

; ================================================================
; Обработчик прерывания UART (Вектор 0x23)
; ================================================================
UART_ISR:
    PUSH ACC
    PUSH PSW

    JNB RI, Check_TI    ; Если не прием, проверяем передачу

    ; --- Прием байта ---
    CLR RI
    MOV A, SBUF

    ; Сохранение в буфер: RX_BUFFER[RX_INDEX] = A
    MOV R0, #RX_BUFFER
    ADD A, RX_INDEX     ; Вычисление смещения
    ADD A, R0
    MOV R0, A           ; R0 = Адрес ячейки
    MOV A, SBUF
    MOV @R0, A

    ; Обновление индекса
    INC RX_INDEX
    MOV A, RX_INDEX
    CJNE A, #4, ISR_Exit ; Ждем 4 байта

    ; Пакет собран полностью
    MOV RX_INDEX, #0

    ; Установка флага FLAGS.0
    MOV A, FLAGS
    ORL A, #01H
    MOV FLAGS, A
    JMP ISR_Exit

Check_TI:
    CLR TI              ; Просто сбрасываем флаг передачи

ISR_Exit:
    POP PSW
    POP ACC
    RETI

; ================================================================
; Процедура обработки команд (Вызывается из Main)
; ================================================================
Process_Command:
    ; 1. Проверка адреса устройства (Байт 0)
    MOV R0, #RX_BUFFER
    MOV A, @R0
    CJNE A, #MY_ADDR, PC_Exit ; Чужой адрес -> выход

    ; 2. Анализ команды (Байт 1)
    INC R0
    MOV A, @R0

    CJNE A, #01H, Check_05
    JMP Cmd_01      ; Ввод данных
Check_05:
    CJNE A, #05H, Check_09
    JMP Cmd_05      ; Сравнение в EEPROM
Check_09:
    CJNE A, #09H, PC_Exit
    JMP Cmd_09      ; Синхросигнал

    ; --- Команда 01: Ввод данных ---
Cmd_01:
    MOV R0, #RX_BUFFER
    INC R0
    INC R0          ; Переход к Д1
    MOV A, @R0
    MOV P1, A       ; Вывод на порт P1
    JMP Send_Ack

    ; --- Команда 05: Работа с EEPROM ---
Cmd_05:
    ; Получаем адреса из пакета
    MOV R0, #RX_BUFFER
    INC R0
    INC R0
    MOV R2, @R0     ; R2 = Адрес 1 (Д1)
    INC R0
    MOV R3, @R0     ; R3 = Адрес 2 (Д2)

    ; Чтение байта 1 из EEPROM
    MOV EECON, #EEMEN ; Вкл. доступ к EEPROM (бит 7)
    MOV DPH, #00H
    MOV DPL, R2
    MOVX A, @DPTR
    MOV R4, A       ; Сохранили знач. 1

    ; Чтение байта 2 из EEPROM
    MOV DPL, R3
    MOVX A, @DPTR
    MOV R5, A       ; Сохранили знач. 2

    MOV EECON, #00H ; Выкл. доступ к EEPROM

    ; Сравнение
    MOV A, R4
    CJNE A, 05H, Not_Equal ; Сравниваем с R5 (в регистре 05H)
    MOV P1, #0FFH   ; Равны - зажечь P1
    JMP Send_Ack
Not_Equal:
    MOV P1, #00H    ; Не равны - погасить P1
    JMP Send_Ack

    ; --- Команда 09: Синхросигнал ---
Cmd_09:
    ; Измерение импульса на P3.5
    JNB P3.5, $     ; Ждем "1"
    SETB TR0        ; Старт таймера 0
    JB P3.5, $      ; Ждем "0"
    CLR TR0         ; Стоп

    ; Запись результата (TH0:TL0) в буфер для отправки
    MOV R0, #RX_BUFFER
    INC R0
    INC R0          ; Позиция Д1
    MOV @R0, TH0
    INC R0          ; Позиция Д2
    MOV @R0, TL0
    JMP Send_Ack

    ; --- Отправка квитанции и пауза ---
Send_Ack:
    MOV R1, #4
    MOV R0, #RX_BUFFER
Ack_Loop:
    MOV SBUF, @R0
    JNB TI, $       ; Ждем окончания передачи
    CLR TI
    INC R0
    DJNZ R1, Ack_Loop

    ; Пауза 10 мс (Требование ТЗ)
    CLR REN         ; Откл. приемник

    MOV R6, #20
Del_Out:
    MOV R7, #230
    DJNZ R7, $
    DJNZ R6, Del_Out

    SETB REN        ; Вкл. приемник

PC_Exit:
    RET
END
