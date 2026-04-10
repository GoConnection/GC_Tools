# 🚀 Go Connection B2B — CALCULADORA B2B 



---

## 📁 Gestão de Leads & CRM Estruturado

A antiga secção de *Notas* foi totalmente reconstruída para funcionar como um CRM de alta performance.

```
[CRM CORE]
├── Campos estruturados
├── Follow-ups com data/hora
├── Priorização automática (Hoje)
└── Exportação CSV (Excel PT)
```

### 🔹 Funcionalidades

- **Campos Atómicos**
  - Título
  - Subtítulo
  - NIPC (NIF)
  - Telefone
  - Descrição

- **Agenda de Follow-ups**
  - Data + Hora de agendamento

- **Priorização Visual**
  - Leads do dia aparecem como: `⚠️ Ligar Hoje`

- **Admin CRM Panel**
  - Endpoint: `/admin/notas`
  - Visualização global em tempo real
  - Filtro por operador (token)

- **Exportação Inteligente**
  - CSV compatível com Excel (PT)
  - Auditoria externa simplificada

---

## 💬 Comunicação & Suporte (Supervisor Hub)

Sistema interno de comunicação em tempo real.

```
[COMMUNICATION ENGINE]
├── Chat 1:1 (Operador ↔ Supervisor)
├── Multi-chat (Admin)
├── Broadcast global
└── Notificações inteligentes
```

### 🔹 Funcionalidades

- **Chat Privado (1-para-1)**
  - Canal exclusivo por operador (token)

- **Supervisor Multitarefa**
  - Alternância entre conversas
  - `🔴 Mensagens não lidas`

- **Broadcast Global**
  - Modo "Megafone"
  - Modal que bloqueia o ecrã dos operadores

- **Notificações**
  - 🔊 Beep via Web Audio API
  - 🔔 Push notifications (Windows / macOS / Android)

---

## 🎨 Interface & UX

Melhorias focadas na experiência do utilizador.

```
[UX/UI]
├── Dark Mode persistente
├── Layout adaptativo
└── Feedback em tempo real
```

### 🔹 Funcionalidades

- **Dark Mode Persistente**
  - Guardado em `localStorage`

- **Layout Adaptativo**
  - Funil de vendas:
    - Lead Fria
    - Negociação
    - Contrato
    - Fechado

- **Feedback Dinâmico**
  - Toasts:
    - ✅ Guardar
    - ❌ Apagar
    - 📤 Enviar

---

## 🛠️ Arquitetura & Preparação SQL

Preparação completa para migração para base de dados relacional.

```
[DATA LAYER]
├── JSON → Estrutura tipo SQL
├── Simulação de tabelas
└── Backend SQL-ready
```

### 🔹 Estrutura de Dados

```
Conversations:
- id
- token_id
- sender
- message
- timestamp
- read_status

Notes:
- id
- token_id
- status
- contact_fields
```

### 🔹 Migração

- Compatível com:
  - SQLAlchemy
  - MySQL

- Lógica já preparada para transição direta

---

## 📦 Estrutura de Ficheiros

```
[PROJECT STRUCTURE]

app.py
  └── Rotas principais (Chat, CRM, Broadcast)

base.html
  └── Core UI (JS: tema, notificações, widgets)

admin_notas.html
  └── Dashboard global do supervisor

chat_sql_ready.json
  └── Base de dados temporária (SQL-like)
```

---

## 📊 Status

```
Branch: Production-Ready
SQL Migration: Pending
Date: 2026-04-10
```
