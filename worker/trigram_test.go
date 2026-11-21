package worker

import (
	"testing"

	cindex "github.com/google/codesearch/index"
	cregexp "github.com/google/codesearch/regexp"
	"github.com/stretchr/testify/require"
)

func TestFoldRegexSyntaxForTrigramEliminatesCaseQOr(t *testing.T) {
	re, err := cregexp.Compile("(?m)(?i)__bench_test_17952415")
	require.NoError(t, err)

	folded := foldRegexSyntaxForTrigram(re.Syntax)
	q := cindex.RegexpQuery(folded)

	// Without folding, /i produces a QAnd with many QOr sub-nodes (8x blowup per 3-letter trigram).
	// With folding, it becomes a single flat QAnd with 0 Sub nodes.
	require.Equal(t, cindex.QAnd, q.Op)
	require.Empty(t, q.Sub)
	require.NotEmpty(t, q.Trigram)

	// Verify letter trigrams are prioritized before pure-digit trigrams.
	prepared := prepareTrigrams(q.Trigram)
	require.NotEmpty(t, prepared)
	// First byte is tok.IdentTrigram; raw trigram starts at index 1.
	firstRaw := prepared[0][1:]
	lastRaw := prepared[len(prepared)-1][1:]
	require.Equal(t, 0, trigramSelectivityScore(firstRaw), "expected letter trigram first, got %q", firstRaw)
	require.Equal(t, 2, trigramSelectivityScore(lastRaw), "expected pure-digit trigram last, got %q", lastRaw)
}

func TestFoldRegexSyntaxPreservesCaseSensitiveRecall(t *testing.T) {
	re, err := cregexp.Compile("(?m)HelloWorld")
	require.NoError(t, err)

	folded := foldRegexSyntaxForTrigram(re.Syntax)
	q := cindex.RegexpQuery(folded)
	require.Equal(t, cindex.QAnd, q.Op)
	require.Contains(t, q.Trigram, "hel")
	require.Contains(t, q.Trigram, "wor")
	require.Contains(t, q.Trigram, "rld")
}
