import * as React from "react";
import { cn } from "@/lib/utils";
import { cva, type VariantProps } from "class-variance-authority";

const badgeVariants = cva(
  "inline-flex items-center rounded-md border px-1.5 py-0.5 text-xs font-mono font-medium tabular-nums transition-colors",
  {
    variants: {
      variant: {
        default: "border-transparent bg-primary text-primary-foreground shadow",
        secondary: "border-transparent bg-secondary text-secondary-foreground",
        outline: "border-border/60 text-foreground",
        up: "border-terminal-up/20 bg-terminal-up/10 text-terminal-up",
        down: "border-terminal-down/20 bg-terminal-down/10 text-terminal-down",
        muted: "border-transparent bg-muted text-muted-foreground",
        gold: "border-primary/30 bg-primary/10 text-primary",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  },
);

export interface BadgeProps
  extends React.HTMLAttributes<HTMLDivElement>,
    VariantProps<typeof badgeVariants> {}

function Badge({ className, variant, ...props }: BadgeProps) {
  return <div className={cn(badgeVariants({ variant }), className)} {...props} />;
}

export { Badge, badgeVariants };
