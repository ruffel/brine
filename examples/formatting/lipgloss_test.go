package formatting_test

import (
	"fmt"

	"github.com/charmbracelet/lipgloss"
)

func Example_appOwnedLipGlossFormatting() {
	badge := lipgloss.NewStyle().Border(lipgloss.NormalBorder()).Render("OK")
	fmt.Println(badge)

	// Output:
	// ┌──┐
	// │OK│
	// └──┘
}
