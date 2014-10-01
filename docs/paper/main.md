# Introduction

In this paper we compare and contrast stream processing DSLs and distributed stream processing frameworks as a means of integrating some of the concepts developed in the context of stream processing DSLs to distributed systems. Our main contributions are:

* A comparison between stream processing DSLs and distributed stream processing frameworks from expressiveness and usability perspective.
* StreamIt style programming model for distributed stream processing.

**Through out the paper we refer Storm, Samza like platforms as *General Data Stream Management Systems*. **

## StreamIt vs General Data Stream Management System

\begin{table*}[t]
  \centering
  \begin{tabulary}{\linewidth}{LLL}
  \firsthline
  Characteristic & StreamIt & General DSMS \\
  \hline
  Virtually infinite sequence of data & Yes & Yes\\
  Independent stream filters & Yes & Yes. Special input source type. \\
  Stable computation pattern & & Yes \\
  Occasional modification of stream structure & Yes & Possible if required. \\
  Occasional out-of-stream communications & Yes & Can be seen in most use cases. \\
  High performance expectations & Yes & Plus scalability. \\
  Statically Types & Yes & No. Dynamic tuple oriented. \\
  Static stream routing & Yes & Dynamic stream partitioning \\
  Synchronous and totally orders & Yes & Mostly asynchronous and partially ordered. Total order is possible with extensions. \\
  Windowing & Tuple based & Tuple based and time based. \\
  \lasthline
  \end{tabulary}
  \caption{StreamIt vs General DSMSs}
  \label{tab:streamitvsgdsms}
\end{table*}
