# 🚀 GC_Tools - Sales Operations Update (V50.1)

Esta atualização transforma o simulador técnico numa ferramenta estratégica de vendas para Call Center, focando-se em psicologia de fecho, produtividade do operador e redução de erros operacionais.

## 🎨 UI/UX & Reorganização Visual
- **Hierarquia de Resultados:** A tabela de **Memória de Cálculo** foi movida para o topo da secção de resultados, permitindo validação imediata dos cálculos antes da análise visual.
- **Input Cleaning:** Remoção de preços de catálogo nos campos de potência do "Cliente" para evitar confusão visual durante a inserção de dados.
- **Gráfico de Barras:** Reposicionado abaixo da memória de cálculo para servir como reforço visual do fecho.

## 🧠 Ferramentas de Psicologia de Vendas & Fecho
- **A/B Testing (Comparação de Cenários):** Novo sistema de "Guardar Cenário" que permite fixar uma simulação no topo da página e compará-la em tempo real com novas variações (ex: simular alteração de potência).
- **Indicador de Desconto Dinâmico:** Selo visual com animação *pulse* que destaca a **% de desconto real** na fatura, oferecendo uma alternativa ao argumento do valor absoluto em Euros.
- **Métrica FOMO (Custo da Inação):** Cálculo automático do "dinheiro perdido nos últimos 6 meses" para criar urgência no cliente.
- **Escudo Anti-Fricção (TAR vs Energia):** Gráfico tipo *donut* que isola visualmente as Taxas de Acesso à Rede (TAR) da Energia Real, protegendo o operador contra objeções sobre impostos estatais.
- **Argumentário Camaleão (BTN vs BTE):** Scripts de fecho dinâmicos que alteram o jargão de venda consoante o segmento selecionado (Foco em meses de borla vs Otimização de custos operacionais).

## ⚡ Produtividade e Formação (Modo Call Center)
- **Post-it Volátil (Notas Isoladas):** Bloco de notas flutuante com persistência local. **Segurança:** As notas são isoladas por `token` de utilizador via URL, garantindo privacidade entre operadores.
- **Modo Rookie (Tooltips Técnicas):** Injeção de ajuda contextual em jargões como kVA, kW, P1-P4 e TAR, facilitando a curva de aprendizagem de novos operadores.
- **Raio-X da Fatura:** Atalho direto para guias visuais de faturas da concorrência para auxílio na navegação em tempo real durante a chamada.
- **Banner de Cross-Selling:** Notificação inteligente após simulação de eletricidade bem-sucedida para incentivar a venda de Gás Natural.

## 🛡️ Regras de Negócio e Segurança
- **Validador de Escalão de Gás:** Lógica de validação automática baseada no consumo (conversão kWh para $m^3$). Emite alertas de erro se o escalão selecionado for inferior ao exigido, prevenindo rejeições de contrato no Backoffice.
- **Integridade de Backend:** Todas as implementações foram realizadas via *Frontend* (HTML/JS), mantendo as camadas de segurança (Azure Key Vault, SQL Server, MSAL) e a arquitetura original do sistema intocáveis.

---
*Atualizado em: 09/Abril/2026*
